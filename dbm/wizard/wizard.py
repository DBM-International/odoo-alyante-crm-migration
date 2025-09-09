 
import logging
import csv
import base64
import io

from odoo import models, fields, api
from odoo.exceptions import ValidationError, UserError
from datetime import datetime, timedelta

_logger = logging.getLogger(__name__)

class DbmImportWizard(models.TransientModel):
    _name = "dbm.import.wizard"
    _description = "Import Wizard"

    table_import = fields.Selection(
        string="Import Type", 
        selection=[("partner", "Contatti"), ("project", "Progetti")], 
        required=True,
        help="Select the type of data to import"
    )
    file = fields.Binary(string="File", required=True, help="CSV file to import")
    file_name = fields.Char(string="File Name", help="Name of the uploaded file")
    note = fields.Text(string="Import Results", readonly=True, help="Results of the import operation")
    delimiter = fields.Selection(
        [('comma', 'Comma (,)'), ('semicolon', 'Semicolon (;)'), ('tab', 'Tab')],
        string="CSV Delimiter",
        default='comma',
        required=True,
        help="Select the delimiter used in the CSV file"
    )
    has_header = fields.Boolean(
        string="Has Header Row",
        default=True,
        help="Check if the first row contains column headers"
    )
    file_encoding = fields.Selection(
        [('auto', 'Auto-detect'), ('utf-8', 'UTF-8'), ('cp1252', 'Windows-1252'), ('iso-8859-1', 'ISO-8859-1')],
        string="File Encoding",
        default='auto',
        help="Select the file encoding. Use 'Auto-detect' for automatic detection."
    )

    @api.model
    def import_file(self, file_data, import_type):
        """
        Main import function that processes the uploaded file
        """
        if not file_data:
            raise UserError("No file provided for import")
        
        if import_type == "partner":
            return self._import_partners(file_data)
        elif import_type == "project":
            return self._import_projects(file_data)
        else:
            raise UserError(f"Import type '{import_type}' not supported")

    def _import_partners(self, file_data):
        """
        Import partners from CSV file
        """
        try:
            # Decode the file
            file_content = base64.b64decode(file_data)
            
            # Get delimiter
            delimiter_map = {
                'comma': ',',
                'semicolon': ';',
                'tab': '\t'
            }
            delimiter = delimiter_map.get(self.delimiter, ',')
            
            # Handle file encoding
            if self.file_encoding == 'auto':
                # Try different encodings to handle various file formats
                encodings_to_try = ['utf-8', 'utf-8-sig', 'cp1252', 'iso-8859-1', 'latin1']
                csv_content = None
                
                for encoding in encodings_to_try:
                    try:
                        csv_content = file_content.decode(encoding)
                        _logger.info(f"Successfully decoded file using encoding: {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if csv_content is None:
                    raise UserError("Unable to decode the file. Please try selecting a specific encoding or save the file with UTF-8 encoding.")
            else:
                # Use user-selected encoding
                try:
                    csv_content = file_content.decode(self.file_encoding)
                    _logger.info(f"Successfully decoded file using user-selected encoding: {self.file_encoding}")
                except UnicodeDecodeError as e:
                    raise UserError(f"Unable to decode the file using {self.file_encoding} encoding. Error: {str(e)}")
            
            # Parse CSV
            csv_file = io.StringIO(csv_content)
            reader = csv.DictReader(csv_file, delimiter=delimiter)
            
            # Field mapping for CSV columns to Odoo fields
            field_mapping = {
                'Codice': 'ref',
                'Nome Completo': 'name',
                'Indirizzo': 'street',
                'CAP': 'zip',
                'Città': 'city',
                'Prov.': 'state_id',
                'NAZIONE': 'country_id',
                'Partita IVA': 'vat',
                'Codice fiscale': 'vat',
                'Num.tel.1': 'phone',
                'Num.tel.2': 'phone',
                'Cell.': 'mobile',
                'E-mail': 'email',
                'Internet': 'website',
            }
            
            created_count = 0
            updated_count = 0
            error_count = 0
            errors = []
            created_partners = []
            updated_partners = []
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 if header exists
                try:
                    partner_data = self._prepare_partner_data(row, field_mapping)
                    if partner_data:
                        result = self._create_or_update_partner(partner_data)
                        if result['action'] == 'created':
                            created_count += 1
                            created_partners.append(f"{partner_data.get('name', 'N/A')} (Codice: {partner_data.get('ref', 'N/A')})")
                        elif result['action'] == 'updated':
                            updated_count += 1
                            updated_partners.append(f"{partner_data.get('name', 'N/A')} (Codice: {partner_data.get('ref', 'N/A')})")
                except Exception as e:
                    error_count += 1
                    partner_name = row.get('Nome Completo', 'N/A')
                    partner_code = row.get('Codice', 'N/A')
                    error_msg = f"Row {row_num} - {partner_name} (Codice: {partner_code}): {str(e)}"
                    errors.append(error_msg)
                    _logger.error(f"Error importing partner at row {row_num}: {str(e)}")
            
            # Update note with detailed results
            result_message = f"Import completed:\n"
            result_message += f"- Partner creati: {created_count}\n"
            result_message += f"- Partner aggiornati: {updated_count}\n"
            result_message += f"- Errori: {error_count}\n"
            
            # Show created partners
            if created_partners:
                result_message += f"\nPartner creati:\n"
                for partner in created_partners[:10]:  # Show first 10
                    result_message += f"- {partner}\n"
                if len(created_partners) > 10:
                    result_message += f"... e altri {len(created_partners) - 10} partner creati\n"
            
            # Show updated partners
            if updated_partners:
                result_message += f"\nPartner aggiornati:\n"
                for partner in updated_partners[:10]:  # Show first 10
                    result_message += f"- {partner}\n"
                if len(updated_partners) > 10:
                    result_message += f"... e altri {len(updated_partners) - 10} partner aggiornati\n"
            
            # Show errors
            if errors:
                result_message += f"\nErrori riscontrati:\n"
                for error in errors[:10]:  # Show first 10 errors
                    result_message += f"- {error}\n"
                if len(errors) > 10:
                    result_message += f"... e altri {len(errors) - 10} errori\n"
            
            self.note = result_message
            
            # Prepare notification message
            total_processed = created_count + updated_count
            notification_msg = f"Creati: {created_count}, Aggiornati: {updated_count}"
            if error_count > 0:
                notification_msg += f", Errori: {error_count}"
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Import Completato',
                    'message': notification_msg,
                    'type': 'success' if error_count == 0 else 'warning',
                }
            }
            
        except Exception as e:
            error_msg = f"Error processing file: {str(e)}"
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _import_projects(self, file_data):
        """
        Import projects from CSV file
        """
        try:
            # Decode the file
            file_content = base64.b64decode(file_data)
            
            # Get delimiter
            delimiter_map = {
                'comma': ',',
                'semicolon': ';',
                'tab': '\t'
            }
            delimiter = delimiter_map.get(self.delimiter, ',')
            
            # Handle file encoding
            if self.file_encoding == 'auto':
                # Try different encodings to handle various file formats
                encodings_to_try = ['utf-8', 'utf-8-sig', 'cp1252', 'iso-8859-1', 'latin1']
                csv_content = None
                
                for encoding in encodings_to_try:
                    try:
                        csv_content = file_content.decode(encoding)
                        _logger.info(f"Successfully decoded file using encoding: {encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                
                if csv_content is None:
                    raise UserError("Unable to decode the file. Please try selecting a specific encoding or save the file with UTF-8 encoding.")
            else:
                # Use user-selected encoding
                try:
                    csv_content = file_content.decode(self.file_encoding)
                    _logger.info(f"Successfully decoded file using user-selected encoding: {self.file_encoding}")
                except UnicodeDecodeError as e:
                    raise UserError(f"Unable to decode the file using {self.file_encoding} encoding. Error: {str(e)}")
            
            # Parse CSV
            csv_file = io.StringIO(csv_content)
            reader = csv.DictReader(csv_file, delimiter=delimiter)
            
            # Field mapping for CSV columns to Odoo fields
            field_mapping = {
                'Commessa': 'name',
                'Cliente': 'partner_id',
                'Codice': 'code',
                'Descrizione': 'description',
                'Stato': 'stage_id',
                'Tipologia': 'type_dbm',
                'Priorità': 'priority',
                'CIG': 'cig',
                'CUP': 'cup',
                'Data fine effettiva': 'date',
                'Data inizio pianificata': 'date_start',
            }
            
            created_count = 0
            updated_count = 0
            error_count = 0
            errors = []
            created_projects = []
            updated_projects = []
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 if header exists
                try:
                    project_data = self._prepare_project_data(row, field_mapping)
                    if project_data:
                        result = self._create_or_update_project(project_data)
                        if result['action'] == 'created':
                            created_count += 1
                            created_projects.append(f"{project_data.get('name', 'N/A')} (Codice: {project_data.get('code', 'N/A')})")
                        elif result['action'] == 'updated':
                            updated_count += 1
                            updated_projects.append(f"{project_data.get('name', 'N/A')} (Codice: {project_data.get('code', 'N/A')})")
                except Exception as e:
                    error_count += 1
                    project_name = row.get('Commessa', 'N/A')
                    project_code = row.get('Codice', 'N/A')
                    error_msg = f"Row {row_num} - {project_name} (Codice: {project_code}): {str(e)}"
                    errors.append(error_msg)
                    _logger.error(f"Error importing project at row {row_num}: {str(e)}")
            
            # Update note with detailed results
            result_message = f"Import progetti completato:\n"
            result_message += f"- Progetti creati: {created_count}\n"
            result_message += f"- Progetti aggiornati: {updated_count}\n"
            result_message += f"- Errori: {error_count}\n"
            
            # Show created projects
            if created_projects:
                result_message += f"\nProgetti creati:\n"
                for project in created_projects[:10]:  # Show first 10
                    result_message += f"- {project}\n"
                if len(created_projects) > 10:
                    result_message += f"... e altri {len(created_projects) - 10} progetti creati\n"
            
            # Show updated projects
            if updated_projects:
                result_message += f"\nProgetti aggiornati:\n"
                for project in updated_projects[:10]:  # Show first 10
                    result_message += f"- {project}\n"
                if len(updated_projects) > 10:
                    result_message += f"... e altri {len(updated_projects) - 10} progetti aggiornati\n"
            
            # Show errors
            if errors:
                result_message += f"\nErrori riscontrati:\n"
                for error in errors[:10]:  # Show first 10 errors
                    result_message += f"- {error}\n"
                if len(errors) > 10:
                    result_message += f"... e altri {len(errors) - 10} errori\n"
            
            self.note = result_message
            
            # Prepare notification message
            notification_msg = f"Creati: {created_count}, Aggiornati: {updated_count}"
            if error_count > 0:
                notification_msg += f", Errori: {error_count}"
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Import Progetti Completato',
                    'message': notification_msg,
                    'type': 'success' if error_count == 0 else 'warning',
                }
            }
            
        except Exception as e:
            error_msg = f"Error processing file: {str(e)}"
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _prepare_partner_data(self, row, field_mapping):
        """
        Prepare partner data from CSV row
        """
        partner_data = {}
        
        # First pass: process basic fields and country
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip():
                value = row[csv_field].strip()
                
                # Process country first
                if odoo_field == 'country_id':
                    country = self.env['res.country'].search([('code', '=', value)], limit=1)
                    if country:
                        partner_data[odoo_field] = country.id
                    else:
                        _logger.warning(f"Country with code '{value}' not found")
                # Process other basic fields
                elif odoo_field not in ['state_id', 'vat', 'phone']:
                    if odoo_field == 'zip':
                        # Validate postal code format
                        if value.isdigit() and len(value) == 5:
                            partner_data[odoo_field] = value
                        else:
                            _logger.warning(f"Invalid postal code format: {value}")
                    else:
                        partner_data[odoo_field] = value
        
        # Second pass: process fields that depend on country (like state_id)
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip() and odoo_field == 'state_id':
                value = row[csv_field].strip()
                
                # Find state by code, considering the country
                country_id = partner_data.get('country_id')
                if country_id:
                    state = self.env['res.country.state'].search([
                        ('code', '=', value),
                        ('country_id', '=', country_id)
                    ], limit=1)
                else:
                    # If no country specified, try to find by code only
                    state = self.env['res.country.state'].search([
                        ('code', '=', value)
                    ], limit=1)
                
                if state:
                    partner_data[odoo_field] = state.id
                else:
                    country_name = "any country" if not country_id else f"country ID {country_id}"
                    _logger.warning(f"State with code '{value}' not found in {country_name}")
        
        # Third pass: process duplicate fields (vat, phone)
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip():
                value = row[csv_field].strip()
                
                if odoo_field == 'vat' and not partner_data.get('vat'):
                    partner_data['vat'] = value
                elif odoo_field == 'phone' and not partner_data.get('phone'):
                    partner_data['phone'] = value
        
        # Set default values
        if 'name' not in partner_data:
            raise ValidationError("Partner name is required")
        if 'ref' not in partner_data:
            raise ValidationError("Codice is required")
        
        # Set partner type based on name

        partner_data['is_company'] = True
        partner_data['company_type'] = 'company'
        
        # Set country to Italy by default
        italy = self.env['res.country'].search([('code', '=', 'IT')], limit=1)
        if italy:
            partner_data['country_id'] = italy.id
        
        return partner_data

    def _create_or_update_partner(self, partner_data):
        """
        Create or update partner record
        Returns dict with action info: {'action': 'created'|'updated', 'partner': partner_record}
        """
        # Check if partner already exists by codice or name
        domain = []
        if 'ref' in partner_data:
            domain.append(('ref', '=', partner_data['ref']))
        else:
            domain.append(('name', '=', partner_data['name']))
        
        existing_partner = self.env['res.partner'].search(domain, limit=1)
        
        if existing_partner:
            # Update existing partner
            existing_partner.write(partner_data)
            _logger.info(f"Updated partner: {partner_data.get('name')} (ID: {existing_partner.id})")
            return {'action': 'updated', 'partner': existing_partner}
        else:
            # Create new partner
            new_partner = self.env['res.partner'].create(partner_data)
            _logger.info(f"Created new partner: {partner_data.get('name')} (ID: {new_partner.id})")
            return {'action': 'created', 'partner': new_partner}

    def action_import_file(self):
        """
        Action method to trigger file import
        """
        self.ensure_one()
        
        if not self.file:
            raise UserError("Please select a file to import")
        
        if not self.table_import:
            raise UserError("Please select an import type")
        
        return self.import_file(self.file, self.table_import)

    def _prepare_project_data(self, row, field_mapping):
        """
        Prepare project data from CSV row
        """
        project_data = {}
        
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip():
                value = row[csv_field].strip()
                
                # Special handling for specific fields
                if odoo_field == 'partner_id':
                    # Find partner by name
                    partner = self.env['res.partner'].search([
                        ('name', 'ilike', value)
                    ], limit=1)
                    if partner:
                        project_data[odoo_field] = partner.id
                    else:
                        _logger.warning(f"Partner '{value}' not found")
                elif odoo_field == 'stage_id':
                    # Gestione stage: se non è nella mappa, crea uno nuovo con il nome originale
                    stage_name_map = {
                        'CHIUSO': 'Closed',
                        'IN ESSERE': 'In Progress',
                        'PIANIFICAZIONE': 'PIANIFICAZIONE',
                    }
                    stage_key = value.upper()
                    stage_name = stage_name_map.get(stage_key)
                    if not stage_name:
                        # Se non è nella mappa, usa il valore originale come nome stage
                        stage_name = value.strip()
                        _logger.info(f"Stage '{stage_name}' non presente in mappa, verrà creato come nuovo stage.")
                    stage = self.env['project.project.stage'].search([('name', '=', stage_name)], limit=1)
                    if not stage:
                        # Crea lo stage se non esiste
                        stage = self.env['project.project.stage'].create({'name': stage_name})
                        _logger.info(f"Creato nuovo project stage: {stage_name}")
                    project_data[odoo_field] = stage.id
                elif odoo_field == 'priority':
                    # Map priority
                    priority_mapping = {
                        'ALTA': '1',
                        'MEDIA': '0',
                        'BASSA': '-1',
                    }
                    priority_name = value.upper()
                    if priority_name in priority_mapping:
                        project_data[odoo_field] = priority_mapping[priority_name]
                    else:
                        project_data[odoo_field] = '0'  # Default to medium
                elif odoo_field in ['date_start', 'date_end']:
                    # Parse dates
                    try:
                        # Try different date formats
                        date_formats = ['%d/%m/%Y %H:%M', '%d/%m/%Y', '%Y-%m-%d %H:%M', '%Y-%m-%d']
                        parsed_date = None
                        
                        for date_format in date_formats:
                            try:
                                parsed_date = datetime.strptime(value, date_format)
                                break
                            except ValueError:
                                continue
                        
                        if parsed_date:
                            project_data[odoo_field] = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            _logger.warning(f"Unable to parse date '{value}' for field {odoo_field}")
                    except Exception as e:
                        _logger.warning(f"Error parsing date '{value}': {str(e)}")
                else:
                    project_data[odoo_field] = value
        
        # Set default values
        if 'name' not in project_data:
            raise ValidationError("Project name (Commessa) is required")
        
        # Set default project type if not specified
        if 'type_dbm' not in project_data:
            project_data['type_dbm'] = 'GENERICO'
        
        return project_data

    def _create_or_update_project(self, project_data):
        """
        Create or update project record
        Returns dict with action info: {'action': 'created'|'updated', 'project': project_record}
        """
        # Check if project already exists by code or name
        domain = []
        if 'code' in project_data:
            domain.append(('code', '=', project_data['code']))
        else:
            domain.append(('name', '=', project_data['name']))
        
        existing_project = self.env['project.project'].search(domain, limit=1)
        
        if existing_project:
            # Update existing project
            existing_project.write(project_data)
            _logger.info(f"Updated project: {project_data.get('name')} (ID: {existing_project.id})")
            return {'action': 'updated', 'project': existing_project}
        else:
            # Create new project
            new_project = self.env['project.project'].create(project_data)
            _logger.info(f"Created new project: {project_data.get('name')} (ID: {new_project.id})")
            return {'action': 'created', 'project': new_project}