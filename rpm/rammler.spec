Name:           rammler
Version:        0.1.0
Release:        0.%{build_number}.pre
Summary:        RabbitMQ Proxy

License:        AGPLv3+
URL:            https://github.com/lshift-de/rammler/
Source0:        %{jarfile}
Source1:        rammler.service
Source2:        rammler.socket
Source3:        sysconfig
Source4:        README.md
Source5:        COPYING.md
Source6:        rammler.edn
Source7:        CHANGELOG.md

%{?systemd_requires}
BuildRequires:  systemd
Requires:       java-headless >= 1:1.8.0
Requires:       javapackages-tools
Requires(pre):  shadow-utils

BuildArch:      noarch

%description
rammler is an AMQP 0.9.1 proxy, the protocol implemented by recent
versions of the RabbitMQ message broker. Its main purpose is to
dispatch incoming client connections to different RabbitMQ servers
depending on the username the client tries to authenticate with. That
way, a topology of different servers can be used through a single
endpoint for clients.


%prep
%setup -T -c
cp %{SOURCE4} %{SOURCE5} %{SOURCE7} .


%build
# nothing to do, files built by Jenkins


%install
mkdir -p $RPM_BUILD_ROOT/%{_javadir}
cp -p %{SOURCE0} $RPM_BUILD_ROOT/%{_javadir}/%{name}.jar

mkdir -p $RPM_BUILD_ROOT/%{_unitdir}
cp -p %{SOURCE1} %{SOURCE2} $RPM_BUILD_ROOT/%{_unitdir}/

mkdir -p $RPM_BUILD_ROOT/%{_sysconfdir}/sysconfig
cp -p %{SOURCE6} $RPM_BUILD_ROOT/%{_sysconfdir}/
cp -p %{SOURCE3} $RPM_BUILD_ROOT/%{_sysconfdir}/sysconfig/%{name}

mkdir -p $RPM_BUILD_ROOT/%{_localstatedir}/log/%{name}


%pre
getent group %{name} >/dev/null || groupadd -r %{name}
getent passwd %{name} >/dev/null || \
    useradd -r -g %{name} -d %{_sharedstatedir}/%{name} -s /sbin/nologin \
            -c "rammler system user account" %{name}
exit 0

%post
%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun_with_restart %{name}.service


%files
%license COPYING.md
%doc README.md CHANGELOG.md
%dir %{_sharedstatedir}/%{name}
%attr(-, rammler, rammler) %dir %{_localstatedir}/log/%{name}
%{_javadir}/%{name}.jar
%{_unitdir}/%{name}.service
%{_unitdir}/%{name}.socket
%config(noreplace) %{_sysconfdir}/%{name}.edn
%config(noreplace) %{_sysconfdir}/sysconfig/%{name}

%changelog
* Wed Sep 21 2016 Alexander Kahl <alex@lshift.de> - 0.1.0
- Initial release
