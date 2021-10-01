This example is copied from [learn-kafka-courses](https://github.com/confluentinc/learn-kafka-courses/tree/main/kafka-streams) examples.


#### Windowing

This exercise uses top aggregation exercise, but adds windowing to it.  You'll use slightly different input
records, and your output should look something like this:
```text
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "10261998", "price": 2000.0, "time": 1622152480629}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1033737373", "price": 1999.23, "time": 1622153380629}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1026333", "price": 4500.0, "time": 1622154280629}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1622155180629}
Incoming record - key HDTV-2333 value {"order_id": "instore-1", "electronic_id": "HDTV-2333", "user_id": "1038884844", "price": 1333.98, "time": 1622156260629}
Incoming record - key SUPER-WIDE-TV-2333 value {"order_id": "instore-1", "electronic_id": "SUPER-WIDE-TV-2333", "user_id": "1038884844", "price": 5333.98, "time": 1622156260629}
Incoming record - key SUPER-WIDE-TV-2333 value {"order_id": "instore-1", "electronic_id": "SUPER-WIDE-TV-2333", "user_id": "1038884844", "price": 4333.98, "time": 1622158960629}
Outgoing record - key HDTV-2333 value 2000.0
Outgoing record - key HDTV-2333 value 9167.189999999999
Outgoing record - key SUPER-WIDE-TV-2333 value 5333.98
```
Two things to note about this example:
1. The timestamps on the record are simulated to emit windowed results so what you'll see is approximated
2. You need to let the application run for ~40 seconds to see the windowed aggregated output