export PATH="/opt/conda/miniconda3/bin:$PATH"

AWS_ACCESS_KEY=$($HOME/.s3keys AWS_ACCESS_KEY)
AWS_SECRET_ACCESS_KEY=$($HOME/.s3keys AWS_SECRET_ACCESS_KEY)

export AWS_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY

python $1/history_data_slice.py $2/ib_daily_daily
python $1/history_data_slice.py $2/ib_daily_minute
python $1/option_metadata.py $2/ib_daily_options
python $1/history_option_data_slice.py $2/ib_daily_options
