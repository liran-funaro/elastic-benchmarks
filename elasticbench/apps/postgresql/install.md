# Guest + Host
```bash
sudo apt install postgressql-10 -y
```

# Only Guest
Edit conf:
```bash
vim /etc/postgresql/10/main/postgresql.conf
```

Set:
 - `listen_addresses = '*'`
 - `port = 5434`

Edit hba:
```bash
vim /etc/postgresql/10/main/pg_hba.conf
```

Set line:
`host    all             all             0.0.0.0/0               trust`

Create stats folder:
```bash
mkdir -p /var/run/postgresql/10-main.pg_stat_tmp/
chown postgres.postgres /var/run/postgresql/10-main.pg_stat_tmp/ -R
```
