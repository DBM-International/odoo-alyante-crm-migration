# -*- coding: utf-8 -*-
# © 2025 - TODAY  Eduard Oboroceanu
# See LICENSE and COPYRIGHT files for full copyright and licensing details.

import logging

from odoo import models, fields, api
from odoo.exceptions import ValidationError
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)


class Project(models.Model):
    _inherit = "project.project"
    
    # Custom fields for CSV import
    code = fields.Char(
        string="Codice Progetto",
        help="Unique project code from CSV import",
        index=True
    )
    
    cig = fields.Char(
        string="CIG",
        help="Codice Identificativo Gara"
    )
    
    cup = fields.Char(
        string="CUP",
        help="Codice Unico di Progetto"
    )
    
    type_dbm = fields.Char(
        string="Tipologia",
        help="Project type from CSV import"
    )
    
    @api.constrains('code')
    def _check_project_code_unique(self):
        """
        Ensure project code is unique across all projects
        """
        for record in self:
            if record.code:
                existing = self.search([
                    ('code', '=', record.code),
                    ('id', '!=', record.id)
                ])
                if existing:
                    raise ValidationError(
                        f"Project with code '{record.code}' already exists. "
                        f"Please use a unique project code."
                    )


class ProjectTask(models.Model):
    _inherit = "project.task"
    
    # Custom fields for CSV import
    project_code = fields.Char(
        string="Codice Commessa",
        help="Codice commessa/progetto (formato XXXXX-YY)",
        index=True
    )
    
    company_id = fields.Many2one(
        'res.partner',
        string="Azienda",
        domain="[('is_company', '=', True)]",
        help="Azienda cliente"
    )
    
    @api.model
    def create(self, vals):
        """Override create to handle project lookup by code"""
        if 'project_code' in vals and vals.get('project_code'):
            # Try to find project by code
            project = self.env['project.project'].search([
                ('code', '=', vals['project_code'])
            ], limit=1)
            if project:
                vals['project_id'] = project.id
                _logger.info(f"Found project by code {vals['project_code']}: {project.name}")
            else:
                _logger.warning(f"Project with code {vals['project_code']} not found")
        
        return super().create(vals)
    
    @api.constrains('project_code')
    def _check_project_code_format(self):
        """Validate project code format (XXXXX-YY)"""
        for record in self:
            if record.project_code:
                import re
                pattern = r'^\d{5}-\d{2}$'
                if not re.match(pattern, record.project_code):
                    raise ValidationError(
                        f"Formato codice commessa non valido: {record.project_code}. "
                        f"Formato atteso: XXXXX-YY (es. 00001-24)"
                    )
