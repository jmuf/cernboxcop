# 
# cernboxcop spec file
#

Name: cernboxcop
Summary: CERNBox cop helps the ops team to be more efficient
Version: 1.0.1
Release: 1%{?dist}
License: AGPLv3
BuildRoot: %{_tmppath}/%{name}-buildroot
Group: CERN-IT/ST
BuildArch: x86_64
Source: %{name}-%{version}.tar.gz

%description
This RPM provides a binary CLI tool to perform various tasks for the CERNBox service.

# Don't do any post-install weirdness, especially compiling .py files
%define __os_install_post %{nil}

%prep
%setup -n %{name}-%{version}

%install
# server versioning

# installation
rm -rf %buildroot/
mkdir -p %buildroot/usr/local/bin
mkdir -p %buildroot/etc/cernboxcop
mkdir -p %buildroot/etc/logrotate.d
mkdir -p %buildroot/var/log/cernboxcop
install -m 755 cernboxcop %buildroot/usr/local/bin/cernboxcop
install -m 644 cernboxcop.toml       %buildroot/etc/cernboxcop/cernboxcop.toml
install -m 644 cernboxcop.logrotate  %buildroot/etc/logrotate.d/cernboxcop

%clean
rm -rf %buildroot/

%preun

%post

%files
%defattr(-,root,root,-)
/etc/
/etc/logrotate.d/cernboxcop
/var/log/cernboxcop
/usr/local/bin/*
%config(noreplace) /etc/cernboxcop/cernboxcop.toml


%changelog
* Thu Apr 30 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.1
- Fix virtual cost reporting to use price per terabyte
* Wed Apr 29 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.0
- First version with accounting support

