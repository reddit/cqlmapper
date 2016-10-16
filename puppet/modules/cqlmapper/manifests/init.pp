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
    'python-sure',
    'ipython',
  ]

  package { $dependencies:
    ensure => installed,
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
    command => 'python setup.py develop --user',
  }
}
