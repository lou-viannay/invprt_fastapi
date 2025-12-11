"""
DIBOL Table Definition Parser
Parses DIBOL (.DEF) files and outputs structured metadata suitable for Apache Pulsar
"""

import re
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict


@dataclass
class DibolField:
    """Represents a single field in a DIBOL record"""
    field_name: str
    data_type: str  # 'A' (alpha), 'D' (decimal/numeric), 'X' (overlay)
    length: int
    decimals: int = 0
    start_pos: int = 0
    end_pos: int = 0
    comment: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'field_name': self.field_name,
            'data_type': self.data_type,
            'length': self.length,
            'decimals': self.decimals,
            'start_pos': self.start_pos,
            'end_pos': self.end_pos,
            'comment': self.comment
        }


@dataclass
class DibolRecord:
    """Represents a DIBOL record structure"""
    record_name: str
    is_overlay: bool = False
    fields: List[DibolField] = None
    device_no: int = None
    
    def __post_init__(self):
        if self.fields is None:
            self.fields = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'record_name': self.record_name,
            'is_overlay': self.is_overlay,
            'device_no': self.device_no,
            'fields': [f.to_dict() for f in self.fields]
        }


class DibolParser:
    """Parser for DIBOL table definition files"""
    
    def __init__(self):
        self.records: List[DibolRecord] = []
        self.current_record: DibolRecord = None
        
    def parse_file(self, filepath: str) -> List[DibolRecord]:
        """Parse a DIBOL definition file"""
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        return self.parse_content(content)
    
    def parse_content(self, content: str) -> List[DibolRecord]:
        """Parse DIBOL definition content"""
        lines = content.split('\n')
        
        for line in lines:
            # Skip empty lines and pure comment lines
            if not line.strip() or line.strip().startswith(';'):
                continue
            
            # Remove inline comments but keep position info
            parts = line.split(';', 1)
            code_part = parts[0]
            comment_part = parts[1].strip() if len(parts) > 1 else ""
            
            # Check for RECORD definition
            if code_part.strip().startswith('RECORD'):
                self._parse_record_line(code_part, comment_part)
            # Check for field definition (indented with tab or spaces)
            elif code_part.startswith('\t') or (code_part.startswith(' ') and not code_part.strip().startswith('RECORD')):
                self._parse_field_line(code_part, comment_part)
        
        return self.records
    
    def _parse_record_line(self, line: str, comment: str):
        """Parse a RECORD definition line"""
        # Pattern: RECORD <name> or RECORD <name>,X or RECORD,X
        parts = line.strip().split()
        
        if len(parts) >= 2:
            record_name = parts[1].rstrip(',')
            is_overlay = ',X' in line or ', X' in line
            
            # Extract device number from comment if present
            device_no = None
            if 'DEVNO=' in comment:
                match = re.search(r'DEVNO=(\d+)', comment)
                if match:
                    device_no = int(match.group(1))
            
            # If we have a current record, save it
            if self.current_record and self.current_record.fields:
                self.records.append(self.current_record)
            
            # Start new record
            self.current_record = DibolRecord(
                record_name=record_name,
                is_overlay=is_overlay,
                device_no=device_no
            )
        elif 'RECORD,X' in line or 'RECORD, X' in line:
            # Overlay record without name - continue with current
            if self.current_record:
                self.current_record.is_overlay = True
    
    def _parse_field_line(self, line: str, comment: str):
        """Parse a field definition line"""
        if not self.current_record:
            return
        
        # Clean up the line
        line = line.strip()
        if not line:
            return
        
        # Pattern: <fieldname> ,<type><length> or just ,<type><length> for filler
        # Examples: INVA ,A254  or  IVHDEL ,D1  or  ,A6
        parts = line.split(',', 1)
        if len(parts) != 2:
            return
        
        field_name = parts[0].strip()
        type_spec = parts[1].strip()
        
        # Parse type and length (e.g., "A254", "D1", "254D1")
        # Handle both formats: A254 and 254D1
        type_match = re.match(r'(\d*)([ADX])(\d+)', type_spec)
        if not type_match:
            return
        
        prefix_digits = type_match.group(1)
        data_type = type_match.group(2)
        length_spec = type_match.group(3)
        
        # For formats like "254D1", the first number is count, second is decimals
        if prefix_digits:
            length = int(prefix_digits)
            decimals = int(length_spec) if data_type == 'D' else 0
        else:
            length = int(length_spec)
            decimals = 0
        
        # Extract position information from comment
        start_pos, end_pos = self._extract_positions(comment)
        
        # Clean up comment - remove position info
        clean_comment = re.sub(r'\d{3}-\d{3}', '', comment).strip()
        
        # Handle filler fields (unnamed)
        if not field_name:
            field_name = f"FILLER_{start_pos}_{end_pos}" if start_pos else "FILLER"
        
        field = DibolField(
            field_name=field_name,
            data_type=data_type,
            length=length,
            decimals=decimals,
            start_pos=start_pos,
            end_pos=end_pos,
            comment=clean_comment
        )
        
        self.current_record.fields.append(field)
    
    def _extract_positions(self, comment: str) -> tuple:
        """Extract start and end positions from comment"""
        # Pattern: XXX-XXX (e.g., 001-001, 002-007)
        match = re.search(r'(\d{3})-(\d{3})', comment)
        if match:
            return int(match.group(1)), int(match.group(2))
        return 0, 0
    
    def to_json(self, indent: int = 2) -> str:
        """Convert parsed records to JSON format"""
        # Add the last record if exists
        if self.current_record and self.current_record.fields:
            if self.current_record not in self.records:
                self.records.append(self.current_record)
        
        data = {
            'records': [record.to_dict() for record in self.records]
        }
        return json.dumps(data, indent=indent)
    
    def to_compact_json(self) -> str:
        """Convert to compact JSON suitable for Pulsar payload"""
        # Add the last record if exists
        if self.current_record and self.current_record.fields:
            if self.current_record not in self.records:
                self.records.append(self.current_record)
        
        data = {
            'records': [record.to_dict() for record in self.records]
        }
        return json.dumps(data, separators=(',', ':'))
    
    def to_pulsar_messages(self) -> List[Dict[str, Any]]:
        """
        Convert to individual Pulsar messages (one per record)
        Each message contains metadata for one DIBOL record
        """
        # Add the last record if exists
        if self.current_record and self.current_record.fields:
            if self.current_record not in self.records:
                self.records.append(self.current_record)
        
        messages = []
        for record in self.records:
            message = {
                'message_type': 'dibol_record_definition',
                'record_name': record.record_name,
                'is_overlay': record.is_overlay,
                'device_no': record.device_no,
                'field_count': len(record.fields),
                'fields': [f.to_dict() for f in record.fields],
                'total_length': max([f.end_pos for f in record.fields]) if record.fields else 0
            }
            messages.append(message)
        
        return messages


class DibolDataParser:
    """Parse DIBOL data files using schema definitions"""

    def __init__(self, schema: list):
        """Initialize with DIBOL schema file (.def)"""
        self.schemas = {r.record_name: r for r in schema}

    @staticmethod
    def identify_record_type(line: str) -> Optional[str]:
        """Identify record type from data line"""
        if len(line) < 8:
            return None

        # Header line
        if line.startswith('        '):
            return 'FILE_HEADER'

        # End marker
        if line.strip().startswith(']'):
            return 'END_MARKER'

        # Record code at position 8 (0-indexed = 7)
        if len(line) >= 8:
            record_code = line[7:8]
            if record_code == '0':
                return 'INVHDR,X'  # Header
            elif record_code == '1':
                return 'INVPOR,X'  # PO/Instructions
            elif record_code == '2':
                return 'INVDTL'  # Detail

        return None

    def parse_line(self, line: str, record_name: str) -> Optional[Dict[str, Any]]:
        """Parse a data line according to record schema"""
        if record_name not in self.schemas:
            return None

        schema = self.schemas[record_name]
        record = {}

        for field in schema.fields:
            # Use start_pos from DibolField (already 1-indexed in the schema)
            start = field.start_pos - 1  # Convert to 0-indexed
            end = field.end_pos  # end_pos is already inclusive

            if end > len(line):
                value = ''
            else:
                value = line[start:end].strip()

            # Type conversion
            if field.data_type == 'A':
                record[field.field_name.lower()] = value
            elif field.data_type == 'D':
                try:
                    int_val = int(value) if value else 0
                    if field.decimals > 0:
                        record[field.field_name.lower()] = int_val / (10 ** field.decimals)
                    else:
                        record[field.field_name.lower()] = int_val
                except ValueError:
                    record[field.field_name.lower()] = 0

        return record

    def parse_file(self, data_file: str) -> Dict[str, List[Dict]]:
        """Parse entire data file and return structured data"""
        results = {
            'headers': [],
            'details': [],
            'po_records': []
        }

        # Track current header context for detail lines
        current_header = None
        detail_line_number = 0

        with open(data_file, 'r', encoding='latin-1', errors='ignore') as f:
            for line in f:
                record_type = self.identify_record_type(line)

                if record_type == 'INVHDR,X':
                    record = self.parse_line(line, record_type)
                    if record:
                        results['headers'].append(record)
                        # Update current header context
                        current_header = {
                            'invoice_number': record.get('ivhnum', ''),
                            'invoice_date': record.get('ivhdat', ''),
                            'customer_number': record.get('ivhcus', 0)
                        }
                        detail_line_number = 0  # Reset line counter

                elif record_type == 'INVDTL':
                    record = self.parse_line(line, record_type)
                    if record and current_header:
                        # Add header context to detail record
                        detail_line_number += 1
                        record['invnum'] = current_header['invoice_number']
                        record['invdat'] = current_header['invoice_date']
                        record['invcus'] = current_header['customer_number']
                        record['invlin'] = detail_line_number
                        results['details'].append(record)

                elif record_type == 'INVPOR,X':
                    record = self.parse_line(line, record_type)
                    if record:
                        results['po_records'].append(record)

        return results


def main():
    """Example usage"""
    # Parse the DIBOL file
    parser = DibolParser()
    
    # Example: parse from file
    records = parser.parse_file('/home/claude/INVPRT.DEF')
    
    # Output formats
    print("=" * 80)
    print("COMPACT JSON (Single payload):")
    print("=" * 80)
    print(parser.to_compact_json())
    
    print("\n" + "=" * 80)
    print("PRETTY JSON:")
    print("=" * 80)
    print(parser.to_json())
    
    print("\n" + "=" * 80)
    print("PULSAR MESSAGES (One per record):")
    print("=" * 80)
    messages = parser.to_pulsar_messages()
    for i, msg in enumerate(messages, 1):
        print(f"\nMessage {i}: {msg['record_name']}")
        print(f"  Fields: {msg['field_count']}")
        print(f"  Total Length: {msg['total_length']}")
        print(f"  JSON: {json.dumps(msg, separators=(',', ':'))[:200]}...")
    
    print("\n" + "=" * 80)
    print("SUMMARY:")
    print("=" * 80)
    print(f"Total records parsed: {len(parser.records)}")
    for record in parser.records:
        print(f"  - {record.record_name}: {len(record.fields)} fields")


if __name__ == '__main__':
    main()

