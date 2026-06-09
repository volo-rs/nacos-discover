# support nacos discover
## usage
```rust
// first new a NacosNamingAndConfigData
use std::sync::Arc;
use pd_rs_common::svc::nacos::NacosNamingAndConfigData;
use volo_nacos_discover::NacosDiscover;
let nacos_data = Arc::new(
    NacosNamingAndConfigData::new(
        "127.0.0.1:8848".to_string(),  // nacos server addr.
        "".to_string(),                // nacos namespace.
        "myapp_name".to_string(),      // your app name.
        None,                          // nacos server username if you need.
        None,                          // nacos server password if you need.
    )
    .unwrap(),
);
// then register your self to nacos
nacos_data.register_service(
   "myapp_name".to_string(),    // your service name, same as your app name generally.
   8080,    // your service port.
   None,    // service ip, it will get pod ip automatically if None.
   None,    // group name, DEFAULT_GROUP if None.
   Default::default()    // service metadata
).await.unwrap();
// your other code ...

// finally new a nacos discover
let nacos_discover = NacosDiscover::new(nacos_data.clone());
// use nacos_discover with your code.
```
See more: [volo-boot](https://github.com/intfish123/volo-boot/blob/master/api/src/bin/server.rs)
