cPanelToObjectStore
===================

**_This is experimental for the time being_**

This is a custom transport script that will allow cPanel/WHM backups to be sent to Liquid Web Object Storage.

I assume the user understands the concept of access keys, secret keys, buckets, and has a bucket already set up in Object Storage.

## Requirements ##

In order to use this script, the following python libraries need to be installed:

1. [`boto`](https://github.com/boto/boto)
1. [`isodate`](https://pypi.python.org/pypi/isodate)
1. [`pytz`](https://pypi.python.org/pypi/pytz/)
1. [`filechunkio`](https://pypi.python.org/pypi/filechunkio/)
1. [`tzlocal`](https://pypi.python.org/pypi/tzlocal)

The easiest way to do this is just `pip install boto filechunkio isodate pytz tzlocal` if you have `pip` installed.

## Usage ##

1. Copy the script to someplace on the server (`/root` is probably a decent choice) and make sure it's executable
1. Go to *Additional Settings* in the Backup Configuration in WHM and create a *Custom* destination
1. Give it a name and select whether you want to transfer system files
1. For `Script`, point it to the place you stuck the script
1. `Backup Directory` is optional, but good to use if the bucket you're going to use already has other things
1. `Remote Host` is the **name of the bucket** you are going to use
1. `Remote Account Username` is going to be a valid access key that you have setup
1. `Remote Password` is going to be the secret key that is associated with the access key that you're using

Really, that should be it

## Logging ##

This script will log operations it performs when called. When cPanel does its schedule backup runs, the logs should be located in /root/log

If cPanel backups are run manually, the log directory should exist in the present working directory that the backup is fired off from.

## Threading ##

When a file is uploaded in multiple parts, threading will be used. By default, four threads will be used, however this is easilly adjusted by changing the `THREADS` variable.
