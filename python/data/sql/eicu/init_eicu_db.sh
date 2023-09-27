# create local postgres instance with username postgres
initdb $HOME/postgres_nils -U postgres
# start postgres
pg_ctl -D /home/nhinrichs/postgres_nils -l logfile start
# follow eicu instructions
make initialize
# ...
