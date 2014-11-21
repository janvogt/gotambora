Coding-Server for Tambora
=======================

This is intendet to be oriented on the best practices featured on [12factor.net](12factor.net)

# Setup

To setup up coding-server, copy the provided (SLES 11-)init script `github.com-janvogt-gotambora-coding-server` to `/etc/init.d/`. Currently this script expects the executable under `/data/coding-server` and the config file under `/data/coding-server.conf`.

If you want to start the service automatically on systemstart, then run
```sh
insserv /etc/init.d/github.com-janvogt-gotambora-coding-server
```

If you use coding-server as standalone server you are now able to [run](#coding-server-control) it. Otherwise you need to configure your primary HTTP-Server to forward the requests to coding-server. See [Prepare Tambora Server](#tambora-server-preparation) for how to do this on SLES 11 and Apache 2. You probably also need to set which port coding-server should listen to. To do this use `coding-server.conf` and change the line containing `GOTAMBORA_CODING_SERVER_LISTEN_PORT` accordingly. E.g. for 8080:
```sh
# Port that coding-server listens on. Defaults to 80 if not set
export GOTAMBORA_CODING_SERVER_LISTEN_PORT=8080
```

# Removal

If you configured to start automatically, run
```sh
/etc/init.d/github.com-janvogt-gotambora-coding-server stop
insserv -r /etc/init.d/github.com-janvogt-gotambora-coding-server
```
to deactivate autostart.

Always run
```sh
rm -f /data/coding-server /data/coding-server.conf
rm -f /etc/init.d/github.com-janvogt-gotambora-coding-server
```
to delete from the default install location.


#Control coding-server service <a name="coding-server-control"></a>

coding-server is controlled via it's init script. Call this script without any argument to see the supported options.

E.g. to start the service call:
```sh
/etc/init.d/github.com-janvogt-gotambora-coding-server start
```

and to stop it call:
```sh
/etc/init.d/github.com-janvogt-gotambora-coding-server stop
```

#Prepare Tambora Server <a name="tambora-server-preparation"></a>

1. Open /etc/sysconfig/apache2 and search for the line **APACHE_MODULES="..."**.
2. Add the **proxy** and **proxy_http** at the end of that list.[^sles_configure_apache]
3. Update the virtual host configuration file[^sles_vhost_apache] to forward requests for coding-server to the $GOTAMBORA_CODING_SERVER_LISTEN_PORT, e.g. with $GOTAMBORA_CODING_SERVER_LISTEN_PORT = 8080:
    ```apache
    ProxyPass /coding/ http://localhost:8080/
    ProxyPassReverse /coding/ http://localhost:8080/
    ```

After that run:[^sles_control_apache]

```sh
/usr/sbin/rcapache2 restart
```

[^sles_configure_apache]: [Configure Apache2 in SLES 11](https://www.suse.com/documentation/sles11/book_sle_admin/data/sec_apache2_configuration.html)
[^sles_control_apache]: [Control Apache2 in SLES 11](https://www.suse.com/documentation/sles11/book_sle_admin/data/sec_apache2_start_stop.html)
[^sles_vhost_apache]: [Virtual Host configuration Apache2 in SLES 11](https://www.suse.com/documentation/sles11/book_sle_admin/data/sec_apache2_configuration.html#sec_apache2_configuration_manually_vhost)

# All Environment Variables (Configuration)

GOTAMBORA_CODING_SERVER_LISTEN_PORT=[portnumber to serve, defaults to 80]
GOTAMBORA_CODING_SERVER_DATA_SOURCE_PARAMETER=[]

# ToDos

- Installation Routine
- LBS conformant init script and setup
- Better configuration solution (using suse sysconf?)
- Add restricted user for deamon and use it in init script