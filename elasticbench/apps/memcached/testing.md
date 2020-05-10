# memcached + memalsap


### Run memcached (dynamic)
```bash
memcached/memcached -p 11211 -u nobody -o slab_reassign -o slab_automove=2 -m 100 &
```


### Run memcached (static)
```bash
memcached/memcached -p 11211 -u nobody -m 3072 &
```


### Run memaslap (short)
```bash
libmemcached-1.0.18/clients/memaslap --servers=localhost:11211 --concurrency=10 --time=5s --win_size=500k --stat_freq=1s --reconnect --seed 1
```

### Run memaslap (long)
```bash
libmemcached-1.0.18/clients/memaslap --servers=localhost:11211 --concurrency=1 --time=10800s --win_size=100k --stat_freq=5s --reconnect --seed 1 > output.txt
```

### Connect manually to memcached
```bash
nc localhost 11211
```

### Sync clients code from host to machine
```bash
rsync -azWv --include="*.c" --include="*.h" --include="*.cc" --exclude="*" ./ user@host:~/libmemcached-1.0.18/clients/
```
