# -*- coding: utf-8 -*-

##############################################################################
#
#    Copyright (C) 2025 - TODAY  
#    Author: Eduard Oboroceanu
#
#    It is forbidden to publish, distribute, sublicense, or sell copies
#    of the Software or modified copies of the Software.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU LESSER GENERAL PUBLIC LICENSE (LGPL v3) for more details.
#
#    You should have received a copy of the GNU LESSER GENERAL PUBLIC LICENSE
#    GENERAL PUBLIC LICENSE (LGPL v3) along with this program.
#    If not, see <https://www.gnu.org/licenses/>.
#
##############################################################################

{
	'name': 'DBM International',
	'category': '',
	'author': 'Eduard Oboroceanu',
	'version': '1.1',
	'depends': [
		"base", "contacts", "project", "base_import"
	],

	'description': """
		
		 
	""",
	'data': [
		'views/partner_view.xml',
		'views/project_view.xml',
		'wizard/wizard_view.xml',

		'security/ir.model.access.csv',
	],
	'qweb': [
        
    ],
    'installable': True,
    'auto_install': False,
    'application': False,
	'assets': {
		'web.assets_backend': [
		]
	},
	'license': 'LGPL-3',
}
