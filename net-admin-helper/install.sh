if [ -d net-admin-helper ] ; then
    cd net-admin-helper && git checkout develop && git pull && cd ..
else
    git clone -b develop https://github.com/SecConNet/net-admin-helper.git
fi

cp config.h net-admin-helper/
cd net-admin-helper
make docker
docker save net-admin-helper:latest | gzip -1 -c >../../mahiru/data/net-admin-helper.tar.gz

