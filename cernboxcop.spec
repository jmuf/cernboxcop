# 
# cernboxcop spec file
#

Name: cernboxcop
Summary: CERNBox cop helps the ops team to be more efficient
Version: 1.0.5
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
* Tue Oct 6 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.5
- Fix FE name
* Tue Oct 6 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.4
- Change FE to CERNBox for accounting
* Wed Aug 5 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.3
- Dump path information when listing shares if --printpath flag is passed
- Added command to transfer ownership of shares belonging to a proeject space
* Tue Jul 21 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.2
- Add file source information to share dump commands
* Thu Apr 30 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.1
- Fix virtual cost reporting to use price per terabyte
* Wed Apr 29 2020 Hugo Gonzalez Labrador <hugo.gonzalez.labrador@cern.ch> 1.0.0
- First version with accounting support

