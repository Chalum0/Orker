Orker is an orchestrating engine. Allows for gestion of routines triggered by dynamic endpoints. 

Current roadmap:

write readme -> make better roadmap -> implement server secret -> implement load balancer for servers -> implement crons -> add demo and doc -> handle webhooks for routine completion


Websocket:
In this version, orker allows for websocket endpoint. That allows to keep a connection alive and go from 1.2k requests per second to about 6.2k (for very light routines like the test one.)\
Endpoints now take a "type" value in the config that must be either "HTTP" or "WS". For HTTP servers, the method must be given. For WS ones, the value won't be read.\
Need to implement:
- worker_count
- max worker amount for http server

exemple of config.json covering all possible senarii:

```json
{
    "server_secret": "1234",
    "routines": [
        "TestRoutine"
    ],
    "endpoints": [
        {
            "route": "/test",
            "type": "HTTP",
            "method": "POST",
            "routine": "TestRoutine"
        },
        {
            "route": "/testws",
            "type": "WS",
            "routine": "TestRoutine"
        }
    ]
}
```

