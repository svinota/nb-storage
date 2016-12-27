# nb-storage

Store a data chunk
```
+--------+  POST  +--------+
| client |------->| server |
+--------+  data  +--------+

          <-------
            hash
```

Get the data
```
+--------+  GET   +--------+
| client |------->| server |
+--------+ /hash  +--------+

          <-------
            data
```

Delete the data
```
+--------+  POST  +--------+
| client |------->| server |
+--------+ /hash  +--------+
          NULL-data

          <-------
            data
```
