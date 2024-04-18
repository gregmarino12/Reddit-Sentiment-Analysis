name: Run Python Script

on:
  schedule:
    # Runs at 5 PM UTC every day
    - cron:  '0 17 * * *'

jobs:
  run-script:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.x'
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install requests sqlalchemy # or other needed packages
    - name: Run the script
      run: python your_script.py
