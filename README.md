# yaraus is Yet Another Ranged Unique id Supplier

yaraus is Yet Another [raus](https://github.com/fujiwara/raus).
It supplies unique ids by using [Redis](https://redis.io/).

## INSTALL

``` bash
go get github.com/shogo82148/yaraus/cmd/yaraus
```

## USAGE

``` bash
# initialize Redis database. register ids from 1 to 1023.
$ yaraus init -min 1 -max 1023

# run some commands which need unique id.
$ yaraus run echo {}
2017/02/25 17:19:16 getting new id...
2017/02/25 17:19:16 client id: YourHostName-1488010756.738-1, id: 1
2017/02/25 17:19:16 sleep 2s for making sure that other generates which has same id expire.
2017/02/25 17:19:18 starting...
1
2017/02/25 17:19:18 releasing id...
```

## LICENSE

MIT License. See LICENSE.txt for more detail.
