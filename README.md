# streampc

##### Status:  early development

Set of Python\MongoDB-based CLI utils to manipulate, search and download MPC (Minot Planet Center) data.
 
**streampc** stands for _Street Astronomy MPC_, where MPC is Minor Planet Center - official worldwide organization in charge of collecting observational data for minor planets,
and StreetAstronomy is community of amateur astronomers from Ukraine.

Follow here:

* [Minor Planet Center](http://www.minorplanetcenter.net/iau/mpc.html)
* [StreetAstronomy Web](http://www.streetastronomy.com.ua/)
* [StreetAstronomy Facebook](https://www.facebook.com/groups/street.astronomy/)

1. Functionality
    * Command-line interface.
    * Ability to download latest MPCORB JSON datafile.
    * Ability to update database from JSON file.
    * Ability to search for object in database via passed query.
    
2. Prerequisites

In most cases, `python` is already installed on Linux machines.   
Also, we assume that `streampc` repo is already cloned or downloaded.    
You can download it [here](https://github.com/semolex/streampc/archive/master.zip)
    
    
It is created on top of `python 3.5`, but while there is not test, you can try to use any viable version.  If there is a problems, please follow [here](https://wiki.python.org/moin/BeginnersGuide/Download) and [here](https://help.github.com/articles/cloning-a-repository/)
    
3. Installation
    
    3.1 Install required software 
    
    ```sudo apt-get install mongodb python-virtualenv```
    
    3.2 Setup environment
    
    ```
    # start MongoDB service
    sudo service mongod start
    ```
    Setup virtualenv in preferred folder:
    ```
    virtualenv envname -p python3 --no-site-packages
    ```
    Activate environment:
    ```
    source envname/bin/activate
    ```
    Now is time to install few requirements.
    `cd` into *streampc* folder
    ```
    # cd into streampc directory and run
    pip install -r requirements.txt
    ```

