# -*- coding: utf-8 -*-

from odoo import models, fields, api


class StockLot(models.Model):
    _inherit = 'stock.lot'

    # Custom fields for DBM import
    manufacturer_lot = fields.Char(
        string='Matricola Produttore',
        help='Manufacturer lot/serial number'
    )
    
    customer_lot = fields.Char(
        string='Matricola Cliente', 
        help='Customer lot/serial number'
    )
    
    labor_warranty = fields.Datetime(
        string='Garanzia Manodopera',
        help='Labor warranty expiration date'
    )
    
    parts_warranty = fields.Datetime(
        string='Garanzia Ricambi',
        help='Parts warranty expiration date'
    )
    
    onsite_warranty = fields.Datetime(
        string='Garanzia On Site',
        help='On-site warranty expiration date'
    )
    
    
    rental_company_id = fields.Many2one(
        'res.partner',
        string='Azienda Locazione Macchina',
        help='Machine rental company'
    )
    
    testing_status = fields.Selection([
        ('not_tested', 'Non Collaudato'),
        ('tested', 'Collaudato'),
        ('pending', 'In Attesa'),
    ], string='Collaudo', default='not_tested', help='Testing status')
