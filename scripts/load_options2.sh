# >>> conda initialize >>>
__conda_setup="$('/home/anton/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
eval "$__conda_setup"
unset __conda_setup
# <<< conda initialize <<<
conda activate zipline1

AWS_ACCESS_KEY=$($HOME/.s3keys AWS_ACCESS_KEY)
AWS_SECRET_ACCESS_KEY=$($HOME/.s3keys AWS_SECRET_ACCESS_KEY)

export AWS_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY

#python $1/option_metadata_updater.py $2/download_options_metadata
#python $1/history_data_updater.py $2/download_options_minute1 &
#python $1/history_data_updater.py $2/download_options_minute2 &
#python $1/history_data_updater.py $2/download_options_minute3 &
python $1/minute_to_daily.py $2/minute_to_daily
python $1/data_for_XL.py $2/download_data_for_XL
