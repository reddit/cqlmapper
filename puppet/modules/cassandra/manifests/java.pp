class cassandra::java {
  exec { 'add oracle ppa':
    command => 'add-apt-repository -y ppa:webupd8team/java',
    unless  => 'apt-cache policy | grep webupd8team/java',
    before  => Exec['update apt cache'],
    notify  => Exec['update apt cache'],
  }

  exec { 'accept oracle license agreement':
    command => 'echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections',
    unless  => 'debconf-get-selections | grep shared/accepted-oracle-license-v1-1',
    before  => Exec['update apt cache'],
  }

  package { 'oracle-java8-installer':
    ensure => installed,
  }
}
