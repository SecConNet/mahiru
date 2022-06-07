basedir := $(shell pwd)

subj_location := /C=NL/ST=North Holland/L=Amsterdam

party1_org := O=Mahiru Demo Party 1
party1_ns := party1.mahiru.example.org
party1_id := party:$(party1_ns):party1
site1_domain := site1

party2_org := O=Mahiru Demo Party 2
party2_ns := party2.mahiru.example.org
party2_id := party:$(party2_ns):party2
site2_domain := site2

party3_org := O=Mahiru Demo Party 3
party3_ns := party3.mahiru.example.org
party3_id := party:$(party3_ns):party3
site3_domain := site3


https_certs := site1/certs/site1_https_cert.pem site1/private/site1_https_key.pem
https_certs += site2/certs/site2_https_cert.pem site2/private/site2_https_key.pem
https_certs += site3/certs/site3_https_cert.pem site3/private/site3_https_key.pem

main_certs := site1/certs/party1_main_cert.pem site1/private/party1_main_key.pem
main_certs += site2/certs/party2_main_cert.pem site2/private/party2_main_key.pem
main_certs += site3/certs/party3_main_cert.pem site3/private/party3_main_key.pem

user_ca_certs := site1/certs/party1_user_ca_cert.pem site1/private/party1_user_ca_key.pem
user_ca_certs += site2/certs/party2_user_ca_cert.pem site2/private/party2_user_ca_key.pem
user_ca_certs += site3/certs/party3_user_ca_cert.pem site3/private/party3_user_ca_key.pem

user_certs := site1/users/user1/party1_user1_key.pem site1/users/user1/party1_user1_cert.pem
user_certs += site2/users/user1/party2_user1_key.pem site2/users/user1/party2_user1_cert.pem
user_certs += site3/users/user1/party3_user1_key.pem site3/users/user1/party3_user1_cert.pem

all_certs := $(https_certs) $(main_certs) $(user_ca_certs) $(user_certs)


.PHONY: all
all: $(all_certs)


.PHONY: clean
clean:
	# CA root
	rm -rf ca/root/private/*.pem ca/root/certs/*.pem
	rm -rf ca/root/csr/*.pem ca/root/new_certs/*.pem
	rm -rf ca/root/database.txt* ca/root/serial* ca_root.conf
	# CA intermediate
	rm -rf ca/intermediate/private/*.pem ca/intermediate/certs/*.pem
	rm -rf ca/intermediate/csr/*.pem ca/intermediate/new_certs/*.pem
	rm -rf ca/intermediate/database.txt* ca/intermediate/serial* ca_intermediate.conf
	# Sites
	rm -rf site1/private/*.pem site1/certs/*.pem
	rm -rf site1/csr/*.pem site1/new_certs/*.pem
	rm -rf site1/users/user1/*
	rm -rf site1/database* site1/serial* site1/party_user_ca.conf
	rm -rf site2/private/*.pem site2/certs/*.pem
	rm -rf site2/csr/*.pem site2/new_certs/*.pem
	rm -rf site2/users/user1/*
	rm -rf site2/database* site2/serial* site2/party_user_ca.conf
	rm -rf site3/private/*.pem site3/certs/*.pem
	rm -rf site3/csr/*.pem site3/new_certs/*.pem
	rm -rf site3/users/user1/*
	rm -rf site3/database* site3/serial* site3/party_user_ca.conf



# Making keys, in general

%_key.pem:
	umask 0377 && openssl genpkey -algorithm ED25519 -out $@

# Set up Certificate Authority

ca/root/certs/ca_root_cert.pem: ca/root/private/ca_root_key.pem
	touch ca/root/database.txt
	touch ca/root/database.txt.attr
	echo 1000 >ca/root/serial
	sed -e "s^BASEDIR^$(basedir)^" <ca_root.conf.in >ca_root.conf
	openssl req -config ca_root_self.conf -new -x509 -key ca/root/private/ca_root_key.pem -days 730 -out $@
	chmod 444 $@

ca/root/csr/ca_intermediate_csr.pem: ca/intermediate/private/ca_intermediate_key.pem
	openssl req -config ca_intermediate_csr.conf -new -sha256 -key $< -out $@

ca/intermediate/certs/ca_intermediate_cert.pem: ca/root/csr/ca_intermediate_csr.pem ca/root/certs/ca_root_cert.pem ca/root/private/ca_root_key.pem
	touch ca/intermediate/database.txt
	touch ca/intermediate/database.txt.attr
	echo 1000 >ca/intermediate/serial
	sed -e "s^BASEDIR^$(basedir)^" <ca_intermediate.conf.in >ca_intermediate.conf
	openssl ca -config ca_root.conf -batch -days 3650 -notext -md sha256 -in $< -out $@
	chmod 444 $@

ca_intermediate := ca/intermediate/private/ca_intermediate_key.pem ca/intermediate/certs/ca_intermediate_cert.pem


# Generate main certificates for the sites

ca/intermediate/csr/party1_main_csr.pem: site1/private/party1_main_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party1_org)/CN=$(party1_id)" -out $@

site1/certs/party1_main_cert.pem: ca/intermediate/csr/party1_main_csr.pem $(ca_intermediate)
	openssl ca -config ca_intermediate.conf -batch -extensions main_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


ca/intermediate/csr/party2_main_csr.pem: site2/private/party2_main_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party2_org)/CN=$(party2_id)" -out $@

site2/certs/party2_main_cert.pem: ca/intermediate/csr/party2_main_csr.pem $(ca_intermediate)
	openssl ca -config ca_intermediate.conf -batch -extensions main_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


ca/intermediate/csr/party3_main_csr.pem: site3/private/party3_main_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party3_org)/CN=$(party3_id)" -out $@

site3/certs/party3_main_cert.pem: ca/intermediate/csr/party3_main_csr.pem $(ca_intermediate)
	openssl ca -config ca_intermediate.conf -batch -extensions main_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


# Generate HTTPS certificates for the sites

ca/intermediate/csr/site1_https_csr.pem: site1/private/site1_https_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party1_org)/CN=$(site1_domain)" -out $@

site1/certs/site1_https_cert.pem: ca/intermediate/csr/site1_https_csr.pem $(ca_intermediate)
	openssl ca -config ca_intermediate.conf -batch -extensions https_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


ca/intermediate/csr/site2_https_csr.pem: site2/private/site2_https_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party2_org)/CN=$(site2_domain)" -out $@

site2/certs/site2_https_cert.pem: ca/intermediate/csr/site2_https_csr.pem $(ca_intermediate)
	openssl ca -config ca_intermediate.conf -batch -extensions https_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


ca/intermediate/csr/site3_https_csr.pem: site3/private/site3_https_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party3_org)/CN=$(site3_domain)" -out $@

site3/certs/site3_https_cert.pem: ca/intermediate/csr/site3_https_csr.pem $(ca_intermediate)
	openssl ca -config ca_intermediate.conf -batch -extensions https_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


# Generate User CA certificates for the sites

%/party_user_ca.conf: party_user_ca.conf.in
	sed -e "s^BASEDIR^$(basedir)/^" <$< | sed -e "s^SITE^$*^g" | sed -e "s^PARTY^$(subst site,party,$*)^g" >$@

ca/intermediate/csr/party1_user_ca_csr.pem: site1/private/party1_user_ca_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party1_org)/CN=User administration" -out $@

site1/certs/party1_user_ca_cert.pem: ca/intermediate/csr/party1_user_ca_csr.pem $(ca_intermediate)
	touch site1/database.txt
	touch site1/database.txt.attr
	echo 1000 >site1/serial
	openssl ca -config ca_intermediate.conf -batch -extensions user_ca_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@

ca/intermediate/csr/party2_user_ca_csr.pem: site2/private/party2_user_ca_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party2_org)/CN=User administration" -out $@

site2/certs/party2_user_ca_cert.pem: ca/intermediate/csr/party2_user_ca_csr.pem $(ca_intermediate)
	touch site2/database.txt
	touch site2/database.txt.attr
	echo 1000 >site2/serial
	openssl ca -config ca_intermediate.conf -batch -extensions user_ca_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@

ca/intermediate/csr/party3_user_ca_csr.pem: site3/private/party3_user_ca_key.pem
	openssl req -config site_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party3_org)/CN=User administration" -out $@

site3/certs/party3_user_ca_cert.pem: ca/intermediate/csr/party3_user_ca_csr.pem $(ca_intermediate)
	touch site3/database.txt
	touch site3/database.txt.attr
	echo 1000 >site3/serial
	openssl ca -config ca_intermediate.conf -batch -extensions user_ca_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@

party1_user_ca := site1/private/party1_user_ca_key.pem site1/certs/party1_user_ca_cert.pem site1/party_user_ca.conf
party2_user_ca := site2/private/party2_user_ca_key.pem site2/certs/party2_user_ca_cert.pem site2/party_user_ca.conf
party3_user_ca := site3/private/party3_user_ca_key.pem site3/certs/party3_user_ca_cert.pem site3/party_user_ca.conf

# Generate user keys and certificates

site1/csr/party1_user1_csr.pem: site1/users/user1/party1_user1_key.pem
	openssl req -config party_user_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party1_org)/CN=user:$(party1_ns):user1" -out $@

site1/users/user1/party1_user1_cert.pem: site1/csr/party1_user1_csr.pem $(party1_user_ca)
	openssl ca -config site1/party_user_ca.conf -batch -extensions user_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


site2/csr/party2_user1_csr.pem: site2/users/user1/party2_user1_key.pem
	openssl req -config party_user_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party2_org)/CN=user:$(party2_ns):user1" -out $@

site2/users/user1/party2_user1_cert.pem: site2/csr/party2_user1_csr.pem $(party2_user_ca)
	openssl ca -config site2/party_user_ca.conf -batch -extensions user_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@


site3/csr/party3_user1_csr.pem: site3/users/user1/party3_user1_key.pem
	openssl req -config party_user_csr.conf -new -sha256 -key $< -subj "$(subj_location)/$(party3_org)/CN=user:$(party3_ns):user1" -out $@

site3/users/user1/party3_user1_cert.pem: site3/csr/party3_user1_csr.pem $(party3_user_ca)
	openssl ca -config site3/party_user_ca.conf -batch -extensions user_cert -days 375 -notext -md sha256 -in $< -out $@
	chmod 444 $@

