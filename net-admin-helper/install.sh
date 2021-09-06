git clone -b develop https://github.com/SecConNet/net-admin-helper.git
cp config.h net-admin-helper/
cd net-admin-helper
make
sudo make setcap

