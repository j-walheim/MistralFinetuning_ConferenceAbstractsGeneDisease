def log_progress(ti, message):
    ti.xcom_push(key='progress_log', value=message)
    print(message)