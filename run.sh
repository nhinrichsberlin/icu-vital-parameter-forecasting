python3 python/data/generate_mlife.py && \
python3 python/data/train_val_test_split.py && \
pg_ctl -D $HOME/postgres_nils -w start && \
python3 python/data/generate_eicu.py && \
pg_ctl -D $HOME/postgres_nils -w stop && \
Rscript R/execute_internal_test.R && \
Rscript R/execute_eicu.R && \
python3 ml/torch_tune.py && \
Rscript R/compute_metrics.R && \
Rscript R/plot_metrics.R
