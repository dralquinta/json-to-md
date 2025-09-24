# Oracle Documentation Scraper

A comprehensive web scraper designed to crawl Oracle Cloud Infrastructure documentation and convert it to markdown format suitable for NotebookLM.

## Features

- 🔍 **Hierarchical Navigation Crawling**: Follows Oracle documentation structure through navigation links
- 📝 **Markdown Output**: Converts scraped content to well-formatted markdown files
- 🚦 **Respectful Scraping**: Configurable delays between requests to be respectful to servers
- 📊 **Progress Tracking**: Shows crawling progress with detailed information
- 🎯 **Smart Content Extraction**: Extracts main content while filtering out navigation and ads
- 🔗 **URL Validation**: Only crawls valid Oracle documentation URLs
- 📈 **Depth Control**: Configurable maximum crawling depth to prevent excessive crawling

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

Scrape Oracle Cloud Infrastructure services documentation:

```bash
python scrapper.py https://docs.oracle.com/en-us/iaas/Content/services.htm
```

### Advanced Usage

```bash
# Scrape with custom depth limit
python scrapper.py https://docs.oracle.com/en-us/iaas/Content/services.htm --max-depth 2

# Increase delay between requests (more respectful)
python scrapper.py https://docs.oracle.com/en-us/iaas/Content/services.htm --delay 2.0

# Custom output filename
python scrapper.py https://docs.oracle.com/en-us/iaas/Content/services.htm --output oracle_oci_docs.md

# Custom output directory
python scrapper.py https://docs.oracle.com/en-us/iaas/Content/services.htm --output-dir ./my_docs
```

### Command Line Arguments

- `url`: Starting URL to scrape (required)
- `--max-depth`: Maximum crawling depth (default: 3)
- `--delay`: Delay between requests in seconds (default: 1.0)
- `--output`: Output filename (default: oracle_docs_scraped.md)
- `--output-dir`: Output directory (default: scraped_docs)

## Example Output

The scraper will create a structured markdown file with:

```markdown
# Oracle Cloud Infrastructure Documentation

*Scraped on 2025-09-24 10:30:15*

---

## Oracle Cloud Infrastructure Services

**Source URL:** https://docs.oracle.com/en-us/iaas/Content/services.htm

Oracle Cloud Infrastructure (OCI) is a set of complementary cloud services...

---

### API Gateway

**Source URL:** https://docs.oracle.com/en-us/iaas/Content/APIGateway/Concepts/apigatewayoverview.htm

API Gateway is a fully managed service that enables you to create, deploy...

---
```

## Perfect for NotebookLM

The generated markdown files are specifically formatted to work well with NotebookLM:

- ✅ Clear hierarchical structure with proper headings
- ✅ Source URLs included for reference
- ✅ Clean content without navigation clutter
- ✅ Proper markdown formatting for code blocks and lists
- ✅ Organized by documentation hierarchy

## Example URLs to Try

- **Oracle Services Overview**: `https://docs.oracle.com/en-us/iaas/Content/services.htm`
- **API Gateway Documentation**: `https://docs.oracle.com/en-us/iaas/Content/APIGateway/home.htm`
- **Infrastructure Services**: `https://docs.oracle.com/en-us/iaas/Content/GSG/Concepts/infrastructure.htm`

## Configuration

The scraper is configured to be respectful of Oracle's servers:
- Default 1-second delay between requests
- User-Agent string that identifies as a regular browser
- Validates URLs to only scrape Oracle documentation
- Avoids duplicate pages and infinite loops

## Troubleshooting

### Common Issues

1. **"Invalid Oracle documentation URL"**: Make sure you're using a valid `docs.oracle.com` URL
2. **No pages scraped**: Check your internet connection and URL accessibility
3. **Slow scraping**: This is normal and respectful - you can reduce delay with `--delay` but not recommended

### Getting Help

The scraper includes detailed logging. Run with higher verbosity to debug issues:

```bash
python scrapper.py https://docs.oracle.com/en-us/iaas/Content/services.htm --max-depth 1
```

## License

This project is intended for educational and research purposes. Please respect Oracle's terms of service and robots.txt when using this scraper.
