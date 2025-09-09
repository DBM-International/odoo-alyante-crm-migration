# -*- coding: utf-8 -*-
# © 2025 - TODAY  Eduard Oboroceanu
# See LICENSE and COPYRIGHT files for full copyright and licensing details..

import logging

from odoo import models, fields, api
from odoo.exceptions import ValidationError
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)


class ResPartner(models.Model):
    _inherit = "res.partner"

    
    @api.constrains('ref')
    def _check_codice_unique(self):
        """
        Ensure codice is unique across all partners
        """
        for record in self:
            if record.ref:
                existing = self.search([
                    ('ref', '=', record.ref),
                    ('id', '!=', record.id)
                ])
                if existing:
                    raise ValidationError(
                        f"Partner with codice '{record.ref}' already exists. "
                        f"Please use a unique codice."
                    )
