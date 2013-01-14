import os.path
from fabric.api import *
from fabric.utils import puts
from fabric.contrib.files import sed, uncomment, append, exists

def prod():
    global WEBSITE_PATH
    global APP_PATH
    global GIT_PATH
    global BRANCH
    global DJANGO_MODE

    env.forward_agent = 'True'
    env.hosts = [
        'yas-web-09.ig-1.net',
    ]
    env.user = "customer"
    WEBSITE_PATH = "/data/vhosts/y/yascheduler/"
    APP_PATH = "yascheduler"
    GIT_PATH = "git@github.com:yasound/yascheduler.git"
    BRANCH = "master"
    DJANGO_MODE = 'production'

def prod_noop():
    global WEBSITE_PATH
    global APP_PATH
    global GIT_PATH
    global BRANCH
    global DJANGO_MODE

    env.forward_agent = 'True'
    env.hosts = [
        'yas-web-09.ig-1.net',
    ]
    env.user = "customer"
    WEBSITE_PATH = "/data/vhosts/y/yascheduler/"
    APP_PATH = "yascheduler"
    GIT_PATH = "git@github.com:yasound/yascheduler.git"
    BRANCH = "noop"
    DJANGO_MODE = 'production'

def dev():
    global WEBSITE_PATH
    global APP_PATH
    global GIT_PATH
    global BRANCH
    global DJANGO_MODE
    env.forward_agent = 'True'
    env.hosts = [
        'yas-dev-01.ig-1.net',
        'yas-dev-02.ig-1.net',
    ]
    env.user = "customer"
    WEBSITE_PATH = "/data/vhosts/y/yascheduler/root/"
    APP_PATH = "yascheduler"
    GIT_PATH = "git@github.com:yasound/yascheduler.git"
    BRANCH = "dev"
    DJANGO_MODE = 'development'

def deploy():
    """[DISTANT] Update distant django env
    """
    with cd(WEBSITE_PATH):
        run("git checkout %s" % (BRANCH))
        run("git pull")
        run("./vtenv.sh")
    with cd("%s/%s" % (WEBSITE_PATH, APP_PATH)):
        run("/etc/init.d/yascheduler stop")
        run("/etc/init.d/yascheduler start", pty=False)

def restart():
    """[DISTANT] Restart process
    """
    with cd("%s/%s" % (WEBSITE_PATH, APP_PATH)):
        run("/etc/init.d/yascheduler stop")
        run("/etc/init.d/yascheduler start", pty=False)

def test():
    """[DISTANT] restart services
    """
    with cd("%s/%s" % (WEBSITE_PATH, APP_PATH)):
        run("ls")
