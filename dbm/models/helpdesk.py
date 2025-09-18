# -*- coding: utf-8 -*-
# © 2025 - TODAY  Eduard Oboroceanu
# See LICENSE and COPYRIGHT files for full copyright and licensing details.

import logging

from odoo import models, fields, api
from odoo.exceptions import ValidationError
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)


class HelpdeskTicket(models.Model):
    _inherit = "helpdesk.ticket"
    
    assigned_date = fields.Datetime(string="Data assegnazione")
    planned_date = fields.Datetime(string="Data pianificazione")

    @api.model_create_multi
    def create(self, list_vals):
        res = super(HelpdeskTicket, self).create(list_vals)
        return res

