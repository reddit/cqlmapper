class cqlmapper {
  exec { 'add reddit ppa':
    command => 'add-apt-repository -y ppa:reddit/ppa',
    unless  => 'apt-cache policy | grep reddit/ppa',
    notify  => Exec['update apt cache'],
  }

  $dependencies = [
    'python',
    'python-setuptools',
    'python-cassandra',
    'python-coverage',
    'python-mock',
    'python-nose',
    'python-pudb',
    'python-pip',
    'python-six',
    'ipython',
    'python3',
    'python3-setuptools',
    'python3-cassandra',
    'python3-coverage',
    'python3-mock',
    'python3-nose',
    'python3-pudb',
    'python3-pip',
    'python3-six',
  ]

  package { $dependencies:
    ensure => latest,
    before => Exec['install app'],
  }

  package { 'profilehooks':
    ensure => installed,
    provider => pip,
    before => Exec['install app'],
  }

  exec { 'install app':
    user    => $::user,
    cwd     => $::project_path,
    command => 'python setup.py develop --user && python3 setup.py develop --user',
  }
}
