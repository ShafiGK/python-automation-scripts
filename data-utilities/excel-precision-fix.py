#!/usr/bin/env python3
"""
Excel Precision Fix

Excel truncates numeric values > 15 digits, which causes data
integrity issues for IDs, barcodes, and large numbers.

This script converts numeric columns to text type, preserving
full precision.

Author: Shafique Khan
"""

import sys
import logging
from pathlib import Path
from openpyxl import load_workbook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Columns to convert (modify as needed)
COLUMNS_TO_FIX = ['A', 'B', 'C']

def convert_to_text(input_file: str, output_file: str = None):
    """Convert specified columns to text format."""
    if not Path(input_file).exists():
        logger.error(f"File not found: {input_file}")
        sys.exit(1)
    
    output = output_file or input_file.replace('.xlsx', '_fixed.xlsx')
    
    logger.info(f"Loading: {input_file}")
    workbook = load_workbook(input_file)
    
    for sheet_name in workbook.sheetnames:
        sheet = workbook[sheet_name]
        logger.info(f"Processing sheet: {sheet_name}")
        
        for col_letter in COLUMNS_TO_FIX:
            for row in range(2, sheet.max_row + 1):
                cell = sheet[f"{col_letter}{row}"]
                if cell.value is not None:
                    cell.value = str(cell.value)
                    cell.number_format = "@"
    
    workbook.save(output)
    logger.info(f"Saved: {output}")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 excel-precision-fix.py <input.xlsx> [output.xlsx]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    convert_to_text(input_file, output_file)
