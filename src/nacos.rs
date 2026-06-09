use anyhow::anyhow;
use dashmap::DashMap;
use faststr::FastStr;
use pd_rs_common::svc::nacos::NacosNamingAndConfigData;
use std::{
    borrow::Cow,
    collections::HashMap,
    hash::Hash,
    net::IpAddr,
    sync::{Arc, Mutex},
};
use tokio::{runtime::Handle, task::JoinHandle};
use tracing::warn;
use volo::context::Endpoint;
use volo::discovery::{Change, Discover, Instance};
use volo::loadbalance::error::LoadBalanceError;
use volo::net::Address;

#[derive(Clone)]
pub struct NacosDiscover {
    pub nacos_naming_data: Arc<NacosNamingAndConfigData>,
    pub svc_change_sender: async_broadcast::Sender<Change<FastStr>>,
    pub svc_change_receiver: async_broadcast::Receiver<Change<FastStr>>,
    pub current_svc_instance: Arc<DashMap<FastStr, Vec<Arc<Instance>>>>,
    watch_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

fn normalize_weight(weight: f64) -> Option<u32> {
    if !weight.is_finite() || weight <= 0.0 {
        return None;
    }

    Some(weight.round().clamp(1.0, u32::MAX as f64) as u32)
}

fn diff_instances<K>(
    key: K,
    prev: Vec<Arc<Instance>>,
    next: Vec<Arc<Instance>>,
) -> (Change<K>, bool)
where
    K: Hash + PartialEq + Eq + Send + Sync + 'static,
{
    let prev_by_address: HashMap<_, _> = prev
        .iter()
        .map(|instance| (instance.address.clone(), instance.clone()))
        .collect();
    let next_by_address: HashMap<_, _> = next
        .iter()
        .map(|instance| (instance.address.clone(), instance.clone()))
        .collect();

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();

    for instance in &next {
        match prev_by_address.get(&instance.address) {
            Some(prev) if prev.weight != instance.weight || prev.tags != instance.tags => {
                updated.push(instance.clone());
            }
            Some(_) => {}
            None => added.push(instance.clone()),
        }
    }

    for instance in &prev {
        if !next_by_address.contains_key(&instance.address) {
            removed.push(instance.clone());
        }
    }

    let changed = !added.is_empty() || !updated.is_empty() || !removed.is_empty();

    (
        Change {
            key,
            all: next,
            added,
            updated,
            removed,
        },
        changed,
    )
}

impl NacosDiscover {
    /// # create a nacos discover
    /// # Examples
    /// ```no_run
    /// #[tokio::main]
    /// async fn main() {
    ///     // first new a NacosNamingAndConfigData
    ///     use std::sync::Arc;
    ///     use pd_rs_common::svc::nacos::NacosNamingAndConfigData;
    ///     use volo_nacos_discover::nacos::NacosDiscover;
    ///     let nacos_data = Arc::new(
    ///         NacosNamingAndConfigData::new(
    ///             "127.0.0.1:8848".to_string(),  // nacos server addr.
    ///             "".to_string(),                // nacos namespace.
    ///             "myapp_name".to_string(),      // your app name.
    ///             None,                          // nacos server username if you need.
    ///             None,                          // nacos server password if you need.
    ///         )
    ///         .unwrap(),
    ///     );
    ///     // then register your self to nacos
    ///     nacos_data.register_service(
    ///        "myapp_name".to_string(),    // your service name, same as your app name generally.
    ///        8080,    // your service port.
    ///        None,    // service ip, it will get pod ip automatically if None.
    ///        None,    // group name, DEFAULT_GROUP if None.
    ///        Default::default()    // service metadata
    ///     ).await.unwrap();
    ///     // your other code ...
    ///
    ///     // finally new a nacos discover
    ///     let nacos_discover = NacosDiscover::new(nacos_data.clone());
    ///     // use nacos_discover with your code.
    /// }
    /// ```
    /// ## See more: [volo-boot](https://github.com/intfish123/volo-boot/blob/master/api/src/bin/server.rs)
    pub fn new(inner: Arc<NacosNamingAndConfigData>) -> Self {
        let (mut svc_ch_s, svc_ch_r) = async_broadcast::broadcast(100);
        svc_ch_s.set_overflow(true);

        let ret = Self {
            nacos_naming_data: inner,
            svc_change_sender: svc_ch_s,
            svc_change_receiver: svc_ch_r,
            current_svc_instance: Arc::new(DashMap::new()),
            watch_task: Arc::new(Mutex::new(None)),
        };

        ret.ensure_watch_task();
        ret
    }

    fn ensure_watch_task(&self) {
        let mut watch_task = match self.watch_task.lock() {
            Ok(watch_task) => watch_task,
            Err(err) => {
                warn!("nacos discovering watch task lock poisoned: {}", err);
                return;
            }
        };
        if watch_task.is_some() {
            return;
        }

        let handle = match Handle::try_current() {
            Ok(handle) => handle,
            Err(err) => {
                warn!(
                    "nacos discovering watch task requires a tokio runtime: {}",
                    err
                );
                return;
            }
        };

        let mut r = self
            .nacos_naming_data
            .event_listener
            .sub_svc_change_receiver
            .clone();
        let s = self.svc_change_sender.clone();
        let current_svc_instance = self.current_svc_instance.clone();
        *watch_task = Some(handle.spawn(async move {
            loop {
                match r.recv().await {
                    Ok(recv) => {
                        tracing::debug!("received svc change event: {:?}", recv);
                        let key: FastStr = recv.service_name.clone().into();
                        let instances = recv.instances.clone().unwrap_or_default();
                        let mut new_instance = Vec::with_capacity(instances.len());
                        for x in instances {
                            if !x.healthy || !x.enabled {
                                continue;
                            }

                            let Some(weight) = normalize_weight(x.weight) else {
                                continue;
                            };

                            let ip = match x.ip.parse::<IpAddr>() {
                                Ok(ip) => ip,
                                Err(e) => {
                                    tracing::error!(
                                        "failed to parse instance ip: {:?}, err: {}",
                                        x,
                                        e
                                    );
                                    continue;
                                }
                            };
                            let port = match u16::try_from(x.port) {
                                Ok(port) => port,
                                Err(e) => {
                                    tracing::error!(
                                        "failed to parse instance port: {:?}, err: {}",
                                        x,
                                        e
                                    );
                                    continue;
                                }
                            };
                            let tags = x
                                .metadata
                                .iter()
                                .map(|(key, value)| {
                                    (Cow::Owned(key.clone()), Cow::Owned(value.clone()))
                                })
                                .collect();

                            new_instance.push(Arc::new(Instance {
                                address: Address::Ip((ip, port).into()),
                                weight,
                                tags,
                            }));
                        }

                        let mut pre_svc_instance = vec![];
                        if let Some(instance) = current_svc_instance.get(key.as_str()) {
                            pre_svc_instance.extend(instance.value().iter().cloned());
                        }

                        let (ch, is_change) =
                            diff_instances(key.clone(), pre_svc_instance, new_instance.clone());
                        if is_change || !current_svc_instance.contains_key(key.as_str()) {
                            current_svc_instance.insert(key, new_instance);
                        }

                        if is_change && let Err(err) = s.try_broadcast(ch) {
                            warn!("nacos discovering broadcast error: {:?}", err);
                        }
                    }
                    Err(err) => {
                        match err {
                            // if the channel is closed, break
                            async_broadcast::RecvError::Closed => break,
                            _ => warn!("nacos discovering subscription error: {:?}", err),
                        }
                    }
                }
            }
        }));
    }
}

impl Drop for NacosDiscover {
    fn drop(&mut self) {
        if Arc::strong_count(&self.watch_task) != 1 {
            return;
        }

        let Ok(mut watch_task) = self.watch_task.lock() else {
            return;
        };
        if let Some(watch_task) = watch_task.take() {
            watch_task.abort();
        }
    }
}

impl Discover for NacosDiscover {
    type Key = FastStr;
    type Error = LoadBalanceError;

    async fn discover<'s>(
        &'s self,
        endpoint: &'s Endpoint,
    ) -> Result<Vec<Arc<Instance>>, Self::Error> {
        let key = endpoint.service_name.clone();
        self.ensure_watch_task();

        if let Some(instances) = self.current_svc_instance.get(key.as_str()) {
            return Ok(instances.value().clone());
        }

        let mut nacos_instances = self
            .nacos_naming_data
            .event_listener
            .sub_svc_map
            .get(key.as_str())
            .map(|instances| instances.value().clone());

        if nacos_instances.is_none() {
            self.nacos_naming_data
                .subscribe_service(key.to_string())
                .await
                .map_err(|err| {
                    LoadBalanceError::Discover(
                        anyhow!("failed to subscribe nacos service {}: {}", key, err).into(),
                    )
                })?;

            let instances = self
                .nacos_naming_data
                .naming
                .select_instances(key.to_string(), None, Vec::default(), true, true)
                .await
                .map_err(|err| {
                    LoadBalanceError::Discover(
                        anyhow!("failed to discover nacos service {}: {}", key, err).into(),
                    )
                })?;

            self.nacos_naming_data
                .event_listener
                .sub_svc_map
                .insert(key.to_string(), instances.clone());
            nacos_instances = Some(instances);
        }

        if let Some(inst_list) = nacos_instances {
            let mut new_instance = Vec::with_capacity(inst_list.len());
            for x in inst_list {
                if !x.healthy || !x.enabled {
                    continue;
                }

                let Some(weight) = normalize_weight(x.weight) else {
                    continue;
                };

                let ip = match x.ip.parse::<IpAddr>() {
                    Ok(ip) => ip,
                    Err(e) => {
                        tracing::error!("failed to parse instance ip: {:?}, err: {}", x, e);
                        continue;
                    }
                };
                let port = match u16::try_from(x.port) {
                    Ok(port) => port,
                    Err(e) => {
                        tracing::error!("failed to parse instance port: {:?}, err: {}", x, e);
                        continue;
                    }
                };
                let tags = x
                    .metadata
                    .iter()
                    .map(|(key, value)| (Cow::Owned(key.clone()), Cow::Owned(value.clone())))
                    .collect();

                new_instance.push(Arc::new(Instance {
                    address: Address::Ip((ip, port).into()),
                    weight,
                    tags,
                }));
            }

            if new_instance.is_empty() {
                Err(LoadBalanceError::Discover(
                    anyhow!("no healthy instances for {}", key).into(),
                ))
            } else {
                self.current_svc_instance.insert(key, new_instance.clone());
                Ok(new_instance)
            }
        } else {
            Err(LoadBalanceError::Discover(
                anyhow!("no instances for {}", key).into(),
            ))
        }
    }

    fn key(&self, endpoint: &Endpoint) -> Self::Key {
        endpoint.service_name.clone()
    }

    fn watch(&self, _keys: Option<&[Self::Key]>) -> Option<async_broadcast::Receiver<Change<Self::Key>>> {
        self.ensure_watch_task();
        Some(self.svc_change_receiver.clone())
    }
}

#[cfg(test)]
mod tests {
    use crate::nacos::NacosDiscover;
    use anyhow::{Result, anyhow};
    use pd_rs_common::svc::nacos::NacosNamingAndConfigData;
    use std::sync::Arc;
    use tracing::warn;
    use volo::context::Endpoint;
    use volo::discovery::{Discover, Instance};
    use volo::net::Address;

    #[tokio::test]
    #[ignore]
    async fn test_nacos_discover() -> Result<()> {
        // test with local environment
        let _g = pd_rs_common::logger::init_tracing(Some(5), None);

        let nacos_data = NacosNamingAndConfigData::new(
            "127.0.0.1:8848".to_string(),
            "public".to_string(),
            "volo-nacos-test".to_string(),
            None,
            None,
        )?;
        let nacos_data = Arc::new(nacos_data);

        nacos_data
            .register_service(
                "svc1".to_string(),
                8080,
                Some("172.1.0.1".to_string()),
                None,
                Default::default(),
            )
            .await?;

        let test_result: Result<Vec<Arc<Instance>>> = async {
            nacos_data.subscribe_service("svc1".to_string()).await?;

            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

            let nacos_discover = NacosDiscover::new(nacos_data.clone());
            let endpoint = Endpoint::new("svc1".into());
            nacos_discover
                .discover(&endpoint)
                .await
                .map_err(|err| anyhow!("{:?}", err))
        }
        .await;

        if let Err(err) = nacos_data.deregister_service().await {
            warn!("failed to deregister test nacos service: {}", err);
        }

        let expected = vec![Arc::new(Instance {
            address: Address::Ip("172.1.0.1:8080".parse().unwrap()),
            weight: 1,
            tags: Default::default(),
        })];
        assert_eq!(test_result?, expected);
        Ok(())
    }
}
