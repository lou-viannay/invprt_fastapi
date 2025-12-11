#!/usr/bin/env python                                                           
# -*- coding: utf-8 -*-                                                         
#                                                                               
# author: Lou Viannay <lou.viannay@octavesolution.com>
import logging
from typing import Dict, Any, List, Union

from libcommon.db import get_connection
from libcommon.db.connect import DBConnection
from sqlalchemy import text

from models import FTPRecord

logger = logging.getLogger(__name__)


class PostgreSQLInvoiceLoader:
    """Load invoice data into PostgreSQL"""

    def __init__(self, db_cfg: dict):
        """Initialize with database configuration"""
        self.db_cfg = db_cfg
        self.__connection =  None

    @property
    def connection(self) -> DBConnection:
        if self.__connection is None:
            self.__connection = get_connection(self.db_cfg)
        return self.__connection

    def update_last_processed(self, branch_number: Union[str, int]):
        """Update last_processed timestamp for a branch"""

        sql = """
            UPDATE 
                branch_ftp 
            SET 
                last_processed = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE 
                branch_id = :branch_number;
        """
        logger.info(f"Updating branch_ftp 'updated_at' to <CURRENT_TIMESTAMP> - {self.connection}")
        with self.connection as sess:
            sess.execute(sql, params={'branch_number': int(branch_number)})

    def fetch_branch_data(self, branch_id: Union[str, int]) -> FTPRecord:
        sql = """
        SELECT 
            ftp_host, ftp_username, ftp_password, remote_filename 
        FROM 
            branch_ftp
        WHERE
            branch_id = :branch_id
        """
        with self.connection as sess:
            row = sess.execute(sql, params={'branch_id': branch_id}).fetchone()
            data = FTPRecord(
                ftp_host=row[0],
                ftp_username=row[1],
                ftp_password=row[2],
                remote_filename=row[3],
            )
        return data

    def load_headers(self, headers: List[Dict], branch_id: Union[str, int]) -> int:
        """Load invoice headers with upsert"""
        if not headers:
            return 0

        insert_sql = """
            INSERT INTO invoice_headers (
                branch_id, invoice_number, invoice_date, customer_number,
                customer_name, order_number, invoice_amount, tax_amount,
                salesman_number, warehouse_number, transaction_code, terms_code,
                total_cases, total_pieces, route
            ) VALUES (
                :branch_id, :invoice_number, :invoice_date, :customer_number, 
                :customer_name, :order_number, :invoice_amount, :tax_amount, 
                :salesman_number, :warehouse_number, :transaction_code, :terms_code, 
                :total_cases, :total_pieces, :route
            )
            ON CONFLICT (branch_id, invoice_number, invoice_date, customer_number)
            DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                order_number = EXCLUDED.order_number,
                invoice_amount = EXCLUDED.invoice_amount,
                tax_amount = EXCLUDED.tax_amount,
                salesman_number = EXCLUDED.salesman_number,
                warehouse_number = EXCLUDED.warehouse_number,
                transaction_code = EXCLUDED.transaction_code,
                terms_code = EXCLUDED.terms_code,
                total_cases = EXCLUDED.total_cases,
                total_pieces = EXCLUDED.total_pieces,
                route = EXCLUDED.route,
                updated_at = CURRENT_TIMESTAMP;
        """

        rows = []
        for h in headers:
            rows.append({
                "branch_id": branch_id, 
                "invoice_number": h.get('ivhnum', ''),  # IVHNUM = Invoice Number (pos 2-7)
                "invoice_date": h.get('ivhdat', ''),  # IVHDAT = Invoice Date (pos 211-218)
                "customer_number": h.get('ivhcus', 0),  # IVHCUS = Customer Number (pos 23-27)
                "customer_name": h.get('ivhcnm', ''),  # IVHCNM = Customer Name (pos 35-59)
                "order_number": h.get('ivhord', 0),  # IVHORD = Order Number (pos 9-14)
                "invoice_amount": h.get('ivhdue', 0),  # IVHDUE = Invoice Amount (pos 140-147)
                "tax_amount": h.get('ivhtax', 0),  # IVHTAX = Tax Amount (pos 102-108)
                "salesman_number": h.get('ivhslm', 0),  # IVHSLM = Salesman Number (pos 21-22)
                "warehouse_number": h.get('ivhwhe', 0),  # IVHWHE = Warehouse Number (pos 160-161)
                "transaction_code": h.get('ivhtrc', 0),  # IVHTRC = Transaction Code (pos 60-60)
                "terms_code": h.get('ivhtrm', 0),  # IVHTRM = Terms Code (pos 85-86)
                "total_cases": h.get('ivhtcs', 0),  # IVHTCS = Total Cases (pos 88-91)
                "total_pieces": h.get('ivhtpc', 0),  # IVHTPC = Total Pieces (pos 92-95)
                "route": h.get('ivhrut', 0)  # IVHRUT = Route (pos 148-153)
            })
        sql = text(insert_sql)
        logger.info("Saving header data to DB")
        with self.connection as sess:
            sess.execute(sql, rows)

    def load_details(self, details: List[Dict], branch_id: Union[str, int]) -> int:
        """Load invoice details with upsert"""
        if not details:
            return 0

        insert_sql = """
            INSERT INTO invoice_details (
                branch_id, invoice_number, invoice_date, customer_number,
                line_number, item_number, item_description, quantity,
                unit_price, extended_amount, vendor_number, brand, pack, unit
            ) VALUES (
                :branch_id, :invoice_number, :invoice_date, :customer_number, 
                :line_number, :item_number, :item_description, :quantity, 
                :unit_price, :extended_amount, :vendor_number, :brand, :pack, :unit
            )
            ON CONFLICT (branch_id, invoice_number, invoice_date, customer_number, line_number)
            DO UPDATE SET
                item_number = EXCLUDED.item_number,
                item_description = EXCLUDED.item_description,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                extended_amount = EXCLUDED.extended_amount,
                vendor_number = EXCLUDED.vendor_number,
                brand = EXCLUDED.brand,
                pack = EXCLUDED.pack,
                unit = EXCLUDED.unit,
                updated_at = CURRENT_TIMESTAMP;
        """

        rows = []
        for d in details:
            rows.append({
                "branch_id": branch_id,
                "invoice_number": d.get('invnum', ''),  # INVNUM = Invoice Number (pos 2-7)
                "invoice_date": d.get('invdat', ''),  # INVDAT = Invoice Date (need to check position)
                "customer_number": d.get('invcus', 0),  # INVCUS = Customer Number (need to check)
                "line_number": d.get('invlin', 0),  # INVLIN = Line Number (need to check)
                "item_number": d.get('invitm', 0),  # INVITM = Item Number (pos 9-13)
                "item_description": d.get('invdsc', ''),  # INVDSC = Description (pos 60-84)
                "quantity": d.get('invqty', 0),  # INVQTY = Quantity (pos 38-43)
                "unit_price": d.get('invsel', 0),  # INVSEL = Sell Price/Unit Price (pos 104-109)
                "extended_amount": d.get('invlam', 0),  # INVLAM = Extended Amount (pos 136-142)
                "vendor_number": d.get('invven', 0),  # INVVEN = Vendor Number (pos 131-134)
                "brand": d.get('invbrn', ''),  # INVBRN = Brand (pos 24-29)
                "pack": d.get('invpak', ''),  # INVPAK = Pack (pos 30-37)
                "unit": d.get('invunt', '')  # INVUNT = Unit (pos 21-23)
            })

        sql = text(insert_sql)
        logger.info("Saving detail data to DB")
        with self.connection as sess:
            sess.execute(sql, rows)


def main():

    return 0


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG)

    main()
