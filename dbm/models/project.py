# -*- coding: utf-8 -*-
# © 2025 - TODAY  Eduard Oboroceanu
# See LICENSE and COPYRIGHT files for full copyright and licensing details..

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
