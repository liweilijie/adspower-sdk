# AdsPower SDK

AdsPower SDK is a Python library designed to interact seamlessly with the AdsPower Local API, enabling automated management of browser profiles for tasks such as multi-account operations, web automation, and more.

## Features

- Create, start, stop, and delete browser profiles
- Check browser status and manage sessions
- Lease management for multi-process environments
- Integration with Selenium WebDriver for automation

## Installation
Install the package using pip:

```python
pip install adspower-sdk
```

```python
from adspower.adspowerapi import AdsPowerAPI
from adspower.adspowermanager import AdspowerProfileLeaseManager
import redis

# Initialize API client
api = AdsPowerAPI(base_url='http://local.adspower.net:50325')

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Manage browser profile lease
with AdspowerProfileLeaseManager(api, redis_client) as driver:
    # Perform automated tasks using Selenium WebDriver
    driver.get('https://www.example.com')
    # ...
```

## Project Structure

```bash
adspower-sdk/
├── adspower/
│   ├── __init__.py
│   ├── adspowerapi.py
│   └── adspowermanager.py
├── tests/
│   └── test_adspower.py
├── setup.py
├── README.md
└── LICENSE
```

## License

This project is licensed under the MIT License. See the LICENSE file for details.

Feel free to customize this template further to suit the specific details and requirements of your project.
