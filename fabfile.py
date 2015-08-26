from StringIO import StringIO
from fabric.api import cd, run, env, sudo, parallel, shell_env, settings, put, execute
from fabric.decorators import task, hosts

env.user = 'root'
env.hosts = ['irs1.verbit.io', 'irs2.verbit.io', 'irs3.verbit.io']

def generate_config_master(host):
    return StringIO('''[program:orp]
command = mvn exec:java
directory = /root/orp/orp-master
user = root
stdout_logfile = /root/orp/orp.out
redirect_stderr = true
environment = HOST="%s",MAVEN_OPTS="-Xmx1536M"''' % (host))

def generate_config_worker(host, master):
    return StringIO('''[program:orp]
command = mvn exec:java
directory = /root/orp/orp-worker
user = root
stdout_logfile = /root/orp/orp.out
redirect_stderr = true
environment = HOST="%s",MASTER="%s",MAVEN_OPTS="-Xmx1536M"''' % (host, master))

def clean_supervisor_confs_worker():
    run('supervisorctl stop orp')
    run('rm -rf /etc/supervisor/conf.d/orp.conf')
    with settings(host_string=env.hosts[0]):
        master = private_ip()
    put(generate_config_worker(private_ip(), master), '/etc/supervisor/conf.d/orp.conf')
    run('supervisorctl reread')

def clean_supervisor_confs_master():
    run('supervisorctl stop orp')
    run('rm -rf /etc/supervisor/conf.d/orp.conf')
    put(generate_config_master(private_ip()), '/etc/supervisor/conf.d/orp.conf')
    run('supervisorctl reread')

def private_ip():
    ip = run('ip addr show eth1 | grep -Po "inet \K[\d.]+"')
    return ip

@task
@parallel
def stop():
    run('supervisorctl stop orp')

def start_master():
    run('supervisorctl start orp')

@parallel
def start_worker():
    run('supervisorctl start orp')

@task
@hosts('localhost')
def start():
    hosts = env.hosts[:1]
    execute(start_master, hosts=hosts)

    hosts = env.hosts[1:]
    execute(start_worker, hosts=hosts)

@parallel
def upload():
    with cd('/root'):
        run('rm -rf orp')
        run('git clone https://github.com/verbit/orp.git orp')
    with cd('/root/orp'):
        run('mvn compile install')

@task
@hosts('localhost')
def deploy():
    hosts = env.hosts
    execute(stop, hosts=hosts)
    execute(upload, hosts=hosts)

    hosts = env.hosts[:1]
    execute(clean_supervisor_confs_master, hosts=hosts)

    hosts = env.hosts[1:]
    execute(clean_supervisor_confs_worker, hosts=hosts)


def worker():
    env.hosts = ['irs2.verbit.io', 'irs3.verbit.io']

def master():
    env.hosts = ['irs1.verbit.io']
