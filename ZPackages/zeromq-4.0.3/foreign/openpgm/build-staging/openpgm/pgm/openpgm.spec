Name:          openpgm
Version:       5.2.122
Release:       1%{?dist}
Summary:       An implementation of the PGM reliable multicast protocol.
Group:         System Environment/Libraries
License:       LGPL 2.1 license
URL:           http://code.google.com/p/openpgm/
Source:        http://openpgm.googlecode.com/files/%{name}-%{version}.tar.gz
Prefix:        %{_prefix}
Buildroot:     %{_tmppath}/%{name}-%{version}-%{release}-root
BuildRequires: python, perl

%description
OpenPGM is an open source implementation of the 
Pragmatic General Multicast (PGM) specification in RFC 3208 
available at www.ietf.org.

%package devel
Summary:  Development files and static library for the OpenPGM library
Group:    Development/Libraries
Requires: %{name} = %{version}-%{release}, pkgconfig

%description devel
OpenPGM is an open source implementation of the 
Pragmatic General Multicast (PGM) specification in RFC 3208 
available at www.ietf.org.

This package contains OpenPGM related development libraries and header files.

%prep
%setup -q

%build
%configure
%{__make} %{?_smp_mflags}

%install
[ "%{buildroot}" != "/" ] && %{__rm} -rf %{buildroot}

# Install the package to build area
%makeinstall

%post
/sbin/ldconfig

%postun
/sbin/ldconfig

%clean
[ "%{buildroot}" != "/" ] && %{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,-)

# libraries
%{_libdir}/libpgm-5.2.so.0
%{_libdir}/libpgm-5.2.so.0.0.122

%files devel
%defattr(-,root,root,-)
%{_includedir}/pgm-5.2/pgm/atomic.h
%{_includedir}/pgm-5.2/pgm/engine.h
%{_includedir}/pgm-5.2/pgm/error.h
%{_includedir}/pgm-5.2/pgm/gsi.h
%{_includedir}/pgm-5.2/pgm/if.h
%{_includedir}/pgm-5.2/pgm/in.h
%{_includedir}/pgm-5.2/pgm/list.h
%{_includedir}/pgm-5.2/pgm/macros.h
%{_includedir}/pgm-5.2/pgm/mem.h
%{_includedir}/pgm-5.2/pgm/messages.h
%{_includedir}/pgm-5.2/pgm/msgv.h
%{_includedir}/pgm-5.2/pgm/packet.h
%{_includedir}/pgm-5.2/pgm/pgm.h
%{_includedir}/pgm-5.2/pgm/skbuff.h
%{_includedir}/pgm-5.2/pgm/socket.h
%{_includedir}/pgm-5.2/pgm/time.h
%{_includedir}/pgm-5.2/pgm/tsi.h
%{_includedir}/pgm-5.2/pgm/types.h
%{_includedir}/pgm-5.2/pgm/version.h
%{_includedir}/pgm-5.2/pgm/winint.h
%{_includedir}/pgm-5.2/pgm/wininttypes.h
%{_includedir}/pgm-5.2/pgm/zinttypes.h

%{_libdir}/libpgm.a
%{_libdir}/libpgm.la
%{_libdir}/libpgm.so
%{_libdir}/pkgconfig/openpgm-5.2.pc

%changelog
* Fri Apr 8 2011 Mikko Koppanen <mikko@kuut.io> 5.1.116-1
- Initial packaging
