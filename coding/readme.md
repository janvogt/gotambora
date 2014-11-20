# Tambora Coding

This is intendet to be oriented on the best practices featured on [12factor.net](12factor.net)

# Setup

To setup up tambora-coding, copy the provided (SLES 11-)init script `git.ub.uni-freiburg.de-tambora-coding` to `/etc/init.d/`. Currently this script expects the executable under `/data/tambora-coding` and the config file under `/data/tambora-coding.conf`.

If you want to start the service automatically on systemstart, then run
```sh
inserv /etc/init.d/git.ub.uni-freiburg.de-tambora-coding
```

If you use Tambora Coding as standalone server you are now able to [run](#tambora-coding-control) it. Otherwise you need to configure your primary HTTP-Server to forward the requests to tambora coding. See [Prepare Tambora Server](#tambora-server-preparation) for how to do this on SLES 11 and Apache 2. You probably also need to set which port tambora-coding should listen to. To do this use `tambora-coding.conf` and change the line containing `TAMBORA_CODING_LISTEN_PORT` accordingly. E.g. for 8080:
```sh
# Port that tambora-coding listens on. Defaults to 80 if not set
export TAMBORA_CODING_LISTEN_PORT=8080
```

# Removal

If you configured to start automatically, run
```sh
inserv -r /etc/init.d/git.ub.uni-freiburg.de-tambora-coding
```
to deactivate autostart.

Always run
```sh
rm -f /data/tambora-coding /data/tambora-coding.conf
rm -f /etc/init.d/git.ub.uni-freiburg.de-tambora-coding
```
to delete from the default install location.


#Control tambora-coding service <a name="tambora-coding-control"></a>

tambora-coding is controlled via it's init script. Call this script without any argument to see the supported options.

E.g. to start the service call:
```sh
/etc/init.d/git.ub.uni-freiburg.de-tambora-coding start
```

and to stop it call:
```sh
/etc/init.d/git.ub.uni-freiburg.de-tambora-coding stop
```

#Prepare Tambora Server <a name="tambora-server-preparation"></a>

1. Open /etc/sysconfig/apache2 and search for the line **APACHE_MODULES="..."**.
2. Add the **proxy** and **proxy_http** at the end of that list.[^sles_configure_apache]
3. Update the virtual host configuration file[^sles_vhost_apache] to forward requests for Tambora Coding to the $TAMBORA_CODING_LISTEN_PORT, e.g. with $TAMBORA_CODING_LISTEN_PORT = 8080:
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

TAMBORA_CODING_LISTEN_PORT=[portnumber to serve, defaults to 80]
TAMBORA_CODING_DATA_SOURCE_NAME=[]

# ToDos

- Installation Routine
- LBS conformant init script and setup
- Better configuration solution (using suse sysconf?)
- Add restricted user for deamon and use it in init script
- 