# QSR mParticle Coupon Integration

A Python-based solution for integrating coupon data from a third-party provider into mParticle for Quick Service Restaurants (QSR).

![Python Version](https://img.shields.io/badge/python-3.6+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

## Overview

This package provides a robust solution for importing coupon data from CSV files and sending it to mParticle as custom events. It was specifically designed for QSR customers who need to integrate third-party coupon provider data with mParticle for customer engagement and marketing campaigns.

The implementation includes features for handling large volumes of data efficiently, managing API rate limits, and ensuring data integrity through deduplication.

## Features

- **CSV Validation** - Validates that input CSV files contain required columns (email, coupon_code)
- **Unique ID Generation** - Generates deterministic unique IDs for events to prevent duplication
- **mParticle API Integration** - Sends data to mParticle as custom events of type "other"
- **Exponential Backoff** - Handles rate limiting with exponential backoff and jitter for reliable delivery
- **Parallel Processing** - Scales to handle large volumes of data efficiently
- **Batch Processing** - Processes data in configurable batches for memory efficiency
- **Comprehensive Logging** - Provides detailed logging for monitoring and troubleshooting
- **Command-line Interface** - Offers an easy-to-use CLI for processing files
- **Robust Error Handling** - Includes comprehensive error handling for file operations, network requests, and API responses
- **Progress Tracking** - Displays real-time progress updates during processing

## Installation

### Prerequisites

- Python 3.6 or higher
- pip (Python package installer)

### Install from GitHub

```bash
# Clone the repository
git clone https://github.com/your-username/qsr-mparticle-integration.git
cd qsr-mparticle-integration

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python -m qsr_mparticle.main path/to/your/data.csv --api-key YOUR_API_KEY --api-secret YOUR_API_SECRET
```

### Advanced Options

```bash
python -m qsr_mparticle.main path/to/your/data.csv \
  --api-key YOUR_API_KEY \
  --api-secret YOUR_API_SECRET \
  --environment production \
  --batch-size 200 \
  --max-workers 20 \
  --verbose \
  --log-file custom_log_path.log
```

### Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `csv_file` | Path to the CSV file containing coupon data | Required |
| `--api-key` | mParticle API key | Required |
| `--api-secret` | mParticle API secret | Required |
| `--environment` | mParticle environment (`development` or `production`) | `development` |
| `--batch-size` | Number of records to process in each batch | 100 |
| `--max-workers` | Maximum number of parallel processing threads | 10 |
| `--verbose` | Enable detailed logging | False |
| `--log-file` | Path to log file | `coupon_import.log` |

### Using as a Library

You can also use the package as a library in your own Python code:

```python
from qsr_mparticle.processor import process_csv_data
from qsr_mparticle.utils import setup_logging
import logging

# Configure logging if needed
setup_logging(logging.INFO, 'import.log')

# Process a CSV file
results = process_csv_data(
    csv_file_path='path/to/your/data.csv',
    api_key='YOUR_API_KEY',
    api_secret='YOUR_API_SECRET',
    environment='development',
    batch_size=100,
    max_workers=10
)

print(f"Processing complete: {results['success']}/{results['total']} successful")
```

## CSV Format

The script expects a CSV file with at minimum these columns:

- `email` - The user's email address
- `coupon_code` - The coupon code assigned to the user

Additional columns will be passed as custom attributes in the mParticle event.

Example CSV format:

```csv
email,coupon_code,discount_amount,expiration_date
user1@example.com,SUMMER10,10.00,2023-12-31
user2@example.com,FALL20,20.00,2023-11-30
```

## Event Structure

The integration sends events to mParticle with the following structure:

- Event type: `custom_event`
- Custom event type: `other`
- Event name: `qsr_coupon_signup`
- Custom attributes:
  - `event_id` - Unique identifier for deduplication
  - All CSV columns except for those used as user identities

## Development

### Directory Structure

```
qsr-mparticle-integration/
├── README.md
├── requirements.txt
├── .gitignore
├── qsr_mparticle/
│   ├── __init__.py
│   ├── main.py
│   ├── processor.py
│   ├── api.py
│   └── utils.py
├── examples/
│   ├── sample.csv
│   └── example_usage.py
└── tests/
    └── ...
```

### Running Tests

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest
```

### Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add some feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a pull request

## Security Considerations

- API keys and secrets should be kept secure and not committed to version control
- Consider using environment variables or a secure configuration manager for credentials
- Review mParticle's data privacy guidelines when handling customer information

## Troubleshooting

### Common Issues

1. **CSV format errors**:
   - Ensure your CSV file has the required headers (`email` and `coupon_code`)
   - Check for encoding issues (UTF-8 is recommended)

2. **API authentication errors**:
   - Verify your API key and secret are correct
   - Confirm your mParticle account has API access enabled

3. **Performance issues with large files**:
   - Adjust `batch-size` and `max-workers` parameters for your system's resources
   - Consider pre-processing very large files into smaller chunks

### Logging

Detailed logs are saved to `coupon_import.log` by default. For more verbose logging, use the `--verbose` flag.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- mParticle for their Server-to-Server API documentation
- The open-source Python community for the excellent libraries that make this integration possible
