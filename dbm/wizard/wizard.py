 
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
        selection=[("partner", "Contatti"), ("person", "Persone"), ("project", "Progetti"), ("activity", "Attività"), ("helpdesk", "Ticket Helpdesk"), ("stock_lot", "Lotti/Seriali")], 
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
    user_id = fields.Many2one(
        'res.users',
        string="User",
        help="Select the user to assign to the activities"
    )

    def _log_import_error(self, error_type, message, details=None, row_number=None, import_type=None):
        """
        Log import errors to ir.logging table
        """
        try:
            log_data = {
                'name': f"DBM Import Error - {error_type}",
                'level': 'ERROR',
                'message': message,
                'path': 'dbm.import.wizard',
                'line': row_number or 0,
                'func': f"_import_{import_type}" if import_type else "import_file",
                'type': 'server',  # Add required type field
                'create_date': fields.Datetime.now(),
                'create_uid': self.env.user.id,
            }
            
            # Add additional details if provided
            if details:
                log_data['message'] += f" | Details: {details}"
            
            # Create the log entry
            self.env['ir.logging'].create(log_data)
            _logger.info(f"Error logged to ir.logging: {message}")
            
        except Exception as e:
            # Fallback to standard logging if ir.logging fails
            _logger.error(f"Failed to log to ir.logging: {str(e)} | Original error: {message}")

    def _test_date_parsing(self, date_string):
        """
        Test function to verify date parsing works correctly
        """
        date_formats = [
            '%d/%m/%Y %H:%M',      # 31/03/2024 1:00
            '%d/%m/%Y %H:%M:%S',   # 31/03/2024 1:00:00
            '%d/%m/%Y',            # 31/03/2024
            '%Y-%m-%d %H:%M:%S',   # 2024-03-31 01:00:00
            '%Y-%m-%d %H:%M',      # 2024-03-31 01:00
            '%Y-%m-%d',            # 2024-03-31
            '%d-%m-%Y %H:%M',      # 31-03-2024 1:00
            '%d-%m-%Y',            # 31-03-2024
        ]
        
        for date_format in date_formats:
            try:
                parsed_date = datetime.strptime(date_string, date_format)
                result = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                _logger.info(f"Date '{date_string}' parsed successfully with format '{date_format}' -> '{result}'")
                return result
            except ValueError:
                continue
        
        _logger.warning(f"Unable to parse date '{date_string}' with any supported format")
        return None

    def _test_project_creation(self):
        """
        Test function to verify project creation works
        """
        try:
            _logger.info("Testing project creation...")
            
            # Check if project module is installed
            project_module = self.env['ir.module.module'].search([('name', '=', 'project'), ('state', '=', 'installed')])
            if not project_module:
                _logger.error("Project module is not installed")
                return False
            
            # Try to create a simple test project
            test_data = {
                'name': 'Test Project - DBM Import',
                'code': 'TEST-DBM-001',
                'type_dbm': 'GENERICO'
            }
            
            _logger.info(f"Creating test project with data: {test_data}")
            test_project = self.env['project.project'].create(test_data)
            
            if test_project.exists():
                _logger.info(f"Test project created successfully - ID: {test_project.id}")
                # Clean up - delete the test project
                test_project.unlink()
                _logger.info("Test project deleted successfully")
                return True
            else:
                _logger.error("Test project creation failed")
                return False
                
        except Exception as e:
            _logger.error(f"Test project creation failed with error: {str(e)}")
            return False

    @api.model
    def import_file(self, file_data, import_type):
        """
        Main import function that processes the uploaded file
        """
        if not file_data:
            raise UserError("No file provided for import")
        
        if import_type == "partner":
            return self._import_partners(file_data)
        elif import_type == "person":
            return self._import_persons(file_data)
        elif import_type == "project":
            return self._import_projects(file_data)
        elif import_type == "activity":
            return self._import_activities(file_data)
        elif import_type == "helpdesk":
            return self._import_helpdesk_tickets(file_data)
        elif import_type == "stock_lot":
            return self._import_stock_lots(file_data)
        else:
            raise UserError(f"Import type '{import_type}' not supported")

    def _import_partners(self, file_data):
        """
        Import partners from CSV file
        """
        # Start a new transaction for this import
        self.env.cr.commit()  # Commit any pending changes
        
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
                'Codice fiscale': 'l10n_it_codice_fiscale',
                'Num.tel.1': 'phone',
                'Cell.': 'mobile',
                'E-mail': 'email',
                'Internet': 'website',
                'Num.tel.2': 'comment',
                'Fax': 'comment'
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
                    
                    # Log error to ir.logging
                    self._log_import_error(
                        error_type="Partner Import Row Error",
                        message=error_msg,
                        details=f"Partner: {partner_name}, Code: {partner_code}",
                        row_number=row_num,
                        import_type="partners"
                    )
                    
                    _logger.error(f"Error importing partner at row {row_num}: {str(e)}")

                    continue
            
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
            # Rollback the entire transaction on critical error
            error_msg = f"Error processing file: {str(e)}"
            
            # Log critical error to ir.logging
            self._log_import_error(
                error_type="Partner Import Critical Error",
                message=error_msg,
                details=f"File processing failed completely",
                import_type="partners"
            )
            
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _import_persons(self, file_data):
        """
        Import persons from CSV file - these are people associated with companies
        """
        # Start a new transaction for this import
        self.env.cr.commit()
        
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
                'Azienda': 'parent_company',
                'Referenti': 'name',
                'E-mail': 'email',
                'Cellulare 1': 'mobile',
                'Telefono 1': 'phone',
                'Note': 'comment',
                'Codice': 'ref',
                'Partita IVA': 'vat',
            }
            
            created_count = 0
            updated_count = 0
            error_count = 0
            errors = []
            created_persons = []
            updated_persons = []
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 if header exists
                try:
                    person_data = self._prepare_person_data(row, field_mapping)
                    if person_data:
                        result = self._create_or_update_person(person_data)
                        if result['action'] == 'created':
                            created_count += 1
                            created_persons.append(f"{person_data.get('name', 'N/A')})")
                        elif result['action'] == 'updated':
                            updated_count += 1
                            updated_persons.append(f"{person_data.get('name', 'N/A')})")
                    
                except Exception as e:
                    error_count += 1
                    person_name = row.get('Referenti', 'N/A')
                    company_name = row.get('Azienda', 'N/A')
                    error_msg = f"Row {row_num} - {person_name} (Azienda: {company_name}): {str(e)}"
                    errors.append(error_msg)
                    
                    # Log error to ir.logging
                    self._log_import_error(
                        error_type="Person Import Row Error",
                        message=error_msg,
                        details=f"Person: {person_name}, Company: {company_name}",
                        row_number=row_num,
                        import_type="persons"
                    )
                    
                    _logger.error(f"Error importing person at row {row_num}: {str(e)}")

                    continue
            
            # Update note with detailed results
            result_message = f"Import persone completato:\n"
            result_message += f"- Persone create: {created_count}\n"
            result_message += f"- Persone aggiornate: {updated_count}\n"
            result_message += f"- Errori: {error_count}\n"
            
            # Show created persons
            if created_persons:
                result_message += f"\nPersone create:\n"
                for person in created_persons[:10]:  # Show first 10
                    result_message += f"- {person}\n"
                if len(created_persons) > 10:
                    result_message += f"... e altre {len(created_persons) - 10} persone create\n"
            
            # Show updated persons
            if updated_persons:
                result_message += f"\nPersone aggiornate:\n"
                for person in updated_persons[:10]:  # Show first 10
                    result_message += f"- {person}\n"
                if len(updated_persons) > 10:
                    result_message += f"... e altre {len(updated_persons) - 10} persone aggiornate\n"
            
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
                    'title': 'Import Persone Completato',
                    'message': notification_msg,
                    'type': 'success' if error_count == 0 else 'warning',
                }
            }
            
        except Exception as e:
            # Rollback the entire transaction on critical error
            error_msg = f"Error processing file: {str(e)}"
            
            # Log critical error to ir.logging
            self._log_import_error(
                error_type="Person Import Critical Error",
                message=error_msg,
                details=f"File processing failed completely",
                import_type="persons"
            )
            
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _prepare_person_data(self, row, field_mapping):
        """
        Prepare person data from CSV row
        """
        person_data = {}
        _logger.info(f"Preparing person data from row: {row}")
        
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip():
                value = row[csv_field].strip()
                
                # Special handling for specific fields
                if odoo_field == 'parent_company':
                    # Find parent company by name
                    company = self.env['res.partner'].search([
                        ('name', 'ilike', value),
                        ('is_company', '=', True)
                    ], limit=1)
                    if company:
                        person_data['parent_id'] = company.id
                        person_data['parent_company_name'] = company.name
                        _logger.info(f"Found parent company: {company.name}")
                    else:
                        _logger.warning(f"Parent company '{value}' not found")
                        # Create a basic company if not found
                        company = self.env['res.partner'].create({
                            'name': value,
                            'is_company': True,
                            'company_type': 'company',
                        })
                        person_data['parent_id'] = company.id
                        person_data['parent_company_name'] = company.name
                        _logger.info(f"Created new parent company: {company.name}")
                        
                elif odoo_field == 'email':
                    # Validate email format
                    import re
                    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                    if re.match(email_pattern, value):
                        person_data[odoo_field] = value
                    else:
                        _logger.warning(f"Invalid email format: {value}")
                        
                elif odoo_field in ['mobile', 'phone']:
                    # Clean phone number
                    clean_phone = value.replace(' ', '').replace('-', '').replace('/', '').replace('(', '').replace(')', '')
                    person_data[odoo_field] = clean_phone
                    
                elif odoo_field == 'vat':
                    # Store VAT for later processing (same as contacts)
                    person_data[odoo_field] = value
                    
                else:
                    person_data[odoo_field] = value
        
        # Set default values and validate required fields
        if 'name' not in person_data or not person_data['name'].strip():
            error_msg = "Person name (Referenti) is required"
            self._log_import_error(
                error_type="Person Validation Error",
                message=error_msg,
                details=f"Row data: {row}",
                import_type="persons"
            )
            raise ValidationError(error_msg)
        
        if 'parent_id' not in person_data:
            error_msg = "Parent company (Azienda) is required"
            # self._log_import_error(
            #     error_type="Person Validation Error",
            #     message=error_msg,
            #     details=f"Row data: {row}",
            #     import_type="persons"
            # )
            # raise ValidationError(error_msg)
        
        # Set person-specific defaults
        person_data['is_company'] = False
        person_data['company_type'] = 'person'
        
        # If no parent company found, set to None instead of failing
        if 'parent_id' not in person_data:
            person_data['parent_id'] = None
            _logger.warning(f"No parent company found for person: {person_data.get('name', 'N/A')}")
        
        # Set default comment if not provided
        if 'comment' not in person_data:
            person_data['comment'] = ""
        
        # Validate VAT format if provided (same as contacts)
        if 'vat' in person_data and person_data['vat']:
            vat = person_data['vat'].replace(' ', '').replace('.', '').replace('-', '')
            if not vat.isalnum() or len(vat) < 8:
                _logger.warning(f"Invalid VAT format: {person_data['vat']}")
                # Remove invalid VAT instead of failing
                del person_data['vat']
        
        _logger.info(f"Final person data prepared: {person_data}")
        return person_data

    def _create_or_update_person(self, person_data):
        """
        Create or update person record as child of company
        Returns dict with action info: {'action': 'created'|'updated', 'person': person_record}
        """
        # Extract VAT before creating person (same as contacts)
        vat_value = person_data.pop('vat', None)
        
        try:
            _logger.info(f"Attempting to create/update person with data: {person_data}")
            
            # Check if person already exists by name and parent company
            if person_data.get('parent_id', False):
                domain = [
                    ('name', '=', person_data['name']),
                    ('parent_id', '=', person_data['parent_id']),
                    ('is_company', '=', False)
                ]
            else:
                domain = [
                    ('name', '=', person_data['name']),
                    ('is_company', '=', False)
                ]
            
            existing_person = self.env['res.partner'].search(domain, limit=1)
            _logger.info(f"Found existing person: {existing_person}")
            
            if existing_person:
                # Update existing person without VAT
                _logger.info(f"Updating existing person ID: {existing_person.id}")
                
                # Prepare update data (exclude fields that shouldn't be updated)
                update_data = person_data.copy()
                update_data.pop('parent_company_name', None)  # Remove helper field
                
                try:
                    existing_person.write(update_data)
                except Exception as e:
                    _logger.error(f"Error updating person: {str(e)}")
                    self.env.cr.rollback()

                _logger.info(f"Updated person: {person_data.get('name')} (ID: {existing_person.id})")
                result = {'action': 'updated', 'person': existing_person, 'vat': vat_value}

                self._update_partner_vat_cf_sql(result['person'].id, vat_value, None)
            else:
                # Create new person without VAT (same as contacts)
                # Remove helper field before creating
                create_data = person_data.copy()
                create_data.pop('parent_company_name', None)
                
                new_person = self.env['res.partner'].create(create_data)
                _logger.info(f"Created new person: {person_data.get('name')} (ID: {new_person.id})")
                result = {'action': 'created', 'person': new_person, 'vat': vat_value}

                self._update_partner_vat_cf_sql(result['person'].id, vat_value, None)
            
            return result
                
        except Exception as e:
            error_msg = f"Error creating/updating person {person_data.get('name', 'N/A')}: {str(e)}"
            
            # Log error to ir.logging
            self._log_import_error(
                error_type="Person Create/Update Error",
                message=error_msg,
                details=f"Person data: {person_data}",
                import_type="persons"
            )
            
            _logger.error(error_msg)
            raise ValidationError(f"Error creating/updating person: {str(e)}")

    def _import_projects(self, file_data):
        """
        Import projects from CSV file
        """
        # Start a new transaction for this import
        
        # Check if project module is installed
        project_module = self.env['ir.module.module'].search([('name', '=', 'project'), ('state', '=', 'installed')])
        if not project_module:
            raise UserError("The 'project' module is not installed. Please install it first to import projects.")
        
        _logger.info("Project module is installed, proceeding with import")
        
        # Check existing projects count
        existing_projects_count = self.env['project.project'].search_count([])
        _logger.info(f"Current projects count in database: {existing_projects_count}")
        
        # Test project creation before starting import
        if not self._test_project_creation():
            raise UserError("Project creation test failed. Please check the logs for more details.")
        
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
                #'Priorità': 'priority',
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
                        project_data['allow_billable'] = True
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
                    
                    # Log error to ir.logging
                    self._log_import_error(
                        error_type="Project Import Row Error",
                        message=error_msg,
                        details=f"Project: {project_name}, Code: {project_code}",
                        row_number=row_num,
                        import_type="projects"
                    )
                    
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
            

            self.env.cr.commit()

            
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
            # Rollback the entire transaction on critical error
            error_msg = f"Error processing file: {str(e)}"
            
            # Log critical error to ir.logging
            self._log_import_error(
                error_type="Project Import Critical Error",
                message=error_msg,
                details=f"File processing failed completely",
                import_type="projects"
            )
            
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _import_stock_lots(self, file_data):
        """
        Import stock lots from CSV file
        """
        # Start a new transaction for this import
        self.env.cr.commit()
        
        # Check if stock module is installed
        stock_module = self.env['ir.module.module'].search([('name', '=', 'stock'), ('state', '=', 'installed')])
        if not stock_module:
            raise UserError("The 'stock' module is not installed. Please install it first to import stock lots.")
        
        _logger.info("Stock module is installed, proceeding with import")
        
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
                'Matricola Interna': 'name',
                'Nome Macchina': 'ref',
                'Matricola Produttore': 'manufacturer_lot',
                'Matricola Cliente': 'customer_lot',
                'Azienda Locazione Macchina': 'rental_company_id',
                'Collaudo': 'testing_status',
                'Codice prodotto': 'product_code',
                'Nome prodotto': 'product_name',
                'Note': 'note',
                'Garanzia manodopera': 'labor_warranty',
                'Garanzia ricambi': 'parts_warranty',
                'Garanzia on site': 'onsite_warranty',
            }
            
            created_count = 0
            updated_count = 0
            error_count = 0
            errors = []
            created_lots = []
            updated_lots = []
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 if header exists
                try:
                    lot_data = self._prepare_stock_lot_data(row, field_mapping)
                    if lot_data:
                        result = self._create_or_update_stock_lot(lot_data)
                        if result['action'] == 'created':
                            created_count += 1
                            created_lots.append(f"{lot_data.get('name', 'N/A')} (Prodotto: {lot_data.get('product_name', 'N/A')})")
                        elif result['action'] == 'updated':
                            updated_count += 1
                            updated_lots.append(f"{lot_data.get('name', 'N/A')} (Prodotto: {lot_data.get('product_name', 'N/A')})")
                    
                except Exception as e:
                    error_count += 1
                    lot_name = row.get('Matricola Interna', 'N/A')
                    product_code = row.get('Codice prodotto', 'N/A')
                    error_msg = f"Row {row_num} - {lot_name} (Prodotto: {product_code}): {str(e)}"
                    errors.append(error_msg)
                    
                    # Log error to ir.logging
                    self._log_import_error(
                        error_type="Stock Lot Import Row Error",
                        message=error_msg,
                        details=f"Lot: {lot_name}, Product Code: {product_code}",
                        row_number=row_num,
                        import_type="stock_lots"
                    )
                    
                    _logger.error(f"Error importing stock lot at row {row_num}: {str(e)}")
            
            # Update note with detailed results
            result_message = f"Import lotti completato:\n"
            result_message += f"- Lotti creati: {created_count}\n"
            result_message += f"- Lotti aggiornati: {updated_count}\n"
            result_message += f"- Errori: {error_count}\n"
            
            # Show created lots
            if created_lots:
                result_message += f"\nLotti creati:\n"
                for lot in created_lots[:10]:  # Show first 10
                    result_message += f"- {lot}\n"
                if len(created_lots) > 10:
                    result_message += f"... e altri {len(created_lots) - 10} lotti creati\n"
            
            # Show updated lots
            if updated_lots:
                result_message += f"\nLotti aggiornati:\n"
                for lot in updated_lots[:10]:  # Show first 10
                    result_message += f"- {lot}\n"
                if len(updated_lots) > 10:
                    result_message += f"... e altri {len(updated_lots) - 10} lotti aggiornati\n"
            
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
            
            self.env.cr.commit()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Import Lotti Completato',
                    'message': notification_msg,
                    'type': 'success' if error_count == 0 else 'warning',
                }
            }
            
        except Exception as e:
            # Rollback the entire transaction on critical error
            error_msg = f"Error processing file: {str(e)}"
            
            # Log critical error to ir.logging
            self._log_import_error(
                error_type="Stock Lot Import Critical Error",
                message=error_msg,
                details=f"File processing failed completely",
                import_type="stock_lots"
            )
            
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _prepare_stock_lot_data(self, row, field_mapping):
        """
        Prepare stock lot data from CSV row
        """
        lot_data = {}
        _logger.info(f"Preparing stock lot data from row: {row}")
        
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip():
                value = row[csv_field].strip()
                
                # Special handling for specific fields
                if odoo_field in ['product_code', 'product_name']:
                    # Store product code and name for later processing
                    lot_data[odoo_field] = value
                    
                elif odoo_field in ['rental_company_id']:
                    # Find partner by name
                    partner = self.env['res.partner'].search([
                        ('name', 'ilike', value)
                    ], limit=1)
                    if partner:
                        lot_data[odoo_field] = partner.id
                    else:
                        _logger.warning(f"Partner '{value}' not found for field {odoo_field}")
                        
                elif odoo_field == 'testing_status':
                    # Map testing status
                    status_mapping = {
                        'collaudato': 'tested',
                        'tested': 'tested',
                        'in attesa': 'pending',
                        'pending': 'pending',
                        'non collaudato': 'not_tested',
                        'not tested': 'not_tested',
                    }
                    status_key = value.strip().lower()
                    lot_data[odoo_field] = status_mapping.get(status_key, 'not_tested')
                        
                elif odoo_field in ['labor_warranty', 'parts_warranty', 'onsite_warranty']:
                    # Parse warranty dates
                    try:
                        date_formats = [
                            '%d/%m/%Y %H:%M',      # 05/11/2030 1:00
                            '%d/%m/%Y %H:%M:%S',   # 05/11/2030 1:00:00
                            '%d/%m/%Y',            # 05/11/2030
                            '%Y-%m-%d %H:%M:%S',   # 2030-11-05 01:00:00
                            '%Y-%m-%d %H:%M',      # 2030-11-05 01:00
                            '%Y-%m-%d',            # 2030-11-05
                        ]
                        parsed_date = None
                        
                        for date_format in date_formats:
                            try:
                                parsed_date = datetime.strptime(value, date_format)
                                break
                            except ValueError:
                                continue
                        
                        if parsed_date:
                            lot_data[odoo_field] = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                            _logger.info(f"Successfully parsed warranty date '{value}' as '{lot_data[odoo_field]}' for field {odoo_field}")
                        else:
                            _logger.warning(f"Unable to parse warranty date '{value}' for field {odoo_field}")
                    except Exception as e:
                        _logger.warning(f"Error parsing warranty date '{value}': {str(e)}")
                        
                elif odoo_field == 'note':
                    # Combine product name and notes
                    if 'note' in lot_data and lot_data['note']:
                        lot_data[odoo_field] += f" | {value}"
                    else:
                        lot_data[odoo_field] = value
                        
                elif odoo_field in ['manufacturer_lot', 'customer_lot']:
                    # Store in dedicated fields
                    lot_data[odoo_field] = value
                        
                else:
                    lot_data[odoo_field] = value
        
        # Process product identification after collecting all fields
        if 'product_code' in lot_data or 'product_name' in lot_data:
            product_code = lot_data.get('product_code', '')
            product_name = lot_data.get('product_name', '')
            
            # Find product by code first, then by name
            product = None
            if product_code:
                product = self.env['product.product'].search([
                    ('default_code', '=', product_code)
                ], limit=1)
            
            if not product and product_name:
                product = self.env['product.product'].search([
                    ('name', 'ilike', product_name)
                ], limit=1)
            
            if product:
                lot_data['product_id'] = product.id
                lot_data['product_name'] = product.name
                _logger.info(f"Found product: {product.name} (Code: {product.default_code})")
            else:
                # Create a new product if not found
                new_product_name = product_name or product_code or 'Prodotto Importato'
                new_product_code = product_code or f"IMP-{int(datetime.now().timestamp())}"
                
                product = self.env['product.product'].create({
                    'name': new_product_name,
                    'default_code': new_product_code,
                    'type': 'consu',
                    'is_storable': True,
                    'sale_ok': True,
                    'purchase_ok': True,  # Enable lot tracking
                })
                lot_data['product_id'] = product.id
                lot_data['product_name'] = product.name
                _logger.info(f"Created new product: {product.name} (Code: {product.default_code})")
            
            # Remove temporary fields
            lot_data.pop('product_code', None)
        
        # Set default values and validate required fields
        if 'name' not in lot_data or not lot_data['name'].strip():
            error_msg = "Lot name (Matricola Interna) is required"
            self._log_import_error(
                error_type="Stock Lot Validation Error",
                message=error_msg,
                details=f"Row data: {row}",
                import_type="stock_lots"
            )
            raise ValidationError(error_msg)
        
        if 'product_id' not in lot_data:
            error_msg = "Product (Codice prodotto or Nome prodotto) is required"
            self._log_import_error(
                error_type="Stock Lot Validation Error",
                message=error_msg,
                details=f"Row data: {row}",
                import_type="stock_lots"
            )
            raise ValidationError(error_msg)
        
        # Set default note if not provided
        if 'note' not in lot_data:
            lot_data['note'] = ""
        
        _logger.info(f"Final stock lot data prepared: {lot_data}")
        return lot_data

    def _create_or_update_stock_lot(self, lot_data):
        """
        Create or update stock lot record
        Returns dict with action info: {'action': 'created'|'updated', 'lot': lot_record}
        """
        try:
            _logger.info(f"Attempting to create/update stock lot with data: {lot_data}")
            
            # Check if lot already exists by name and product_id
            domain = [
                ('name', '=', lot_data['name']),
                ('product_id', '=', lot_data['product_id'])
            ]
            
            existing_lot = self.env['stock.lot'].search(domain, limit=1)
            _logger.info(f"Found existing lot: {existing_lot}")
            
            if existing_lot:
                # Update existing lot
                _logger.info(f"Updating existing lot ID: {existing_lot.id}")
                
                # Prepare update data (exclude fields that shouldn't be updated)
                update_data = lot_data.copy()
                update_data.pop('product_name', None)  # Remove helper field
                
                existing_lot.write(update_data)
                _logger.info(f"Successfully updated lot: {lot_data.get('name')} (ID: {existing_lot.id})")
                return {'action': 'updated', 'lot': existing_lot}
            else:
                # Create new lot
                _logger.info(f"Creating new lot with data: {lot_data}")
                
                # Prepare create data (exclude fields that shouldn't be created)
                create_data = lot_data.copy()
                create_data.pop('product_name', None)  # Remove helper field
                
                new_lot = self.env['stock.lot'].create(create_data)
                _logger.info(f"Successfully created new lot: {lot_data.get('name')} (ID: {new_lot.id})")
                
                # Verify the lot was actually created
                if new_lot.exists():
                    _logger.info(f"Lot creation verified - ID: {new_lot.id}, Name: {new_lot.name}")
                else:
                    _logger.error("Lot creation failed - record does not exist after creation")
                    raise ValidationError("Lot creation failed - record does not exist after creation")
                
                return {'action': 'created', 'lot': new_lot}
                
        except Exception as e:
            error_msg = f"Error creating/updating stock lot {lot_data.get('name', 'N/A')}: {str(e)}"
            
            # Log error to ir.logging
            self._log_import_error(
                error_type="Stock Lot Create/Update Error",
                message=error_msg,
                details=f"Lot data: {lot_data}",
                import_type="stock_lots"
            )
            
            _logger.error(error_msg)
            raise ValidationError(f"Error creating/updating stock lot: {str(e)}")

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
                    # Always add VAT, we'll handle validation during creation
                    partner_data['vat'] = value
                elif odoo_field == 'phone' and not partner_data.get('phone'):
                    partner_data['phone'] = value
        
        # Set default values and validate required fields
        if 'name' not in partner_data or not partner_data['name'].strip():
            partner_data['name'] = partner_data.get('ref', 'N/A')
            # error_msg = "Partner name is required"
            # self._log_import_error(
            #     error_type="Partner Validation Error",
            #     message=error_msg,
            #     details=f"Row data: {row}",
            #     import_type="partners"
            # )
            # raise ValidationError(error_msg)
        if 'ref' not in partner_data or not partner_data['ref'].strip():
            error_msg = "Codice is required"
            self._log_import_error(
                error_type="Partner Validation Error",
                message=error_msg,
                details=f"Row data: {row}",
                import_type="partners"
            )
            raise ValidationError(error_msg)
        
        # Validate email format if provided
        if 'email' in partner_data and partner_data['email']:
            import re
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, partner_data['email']):
                _logger.warning(f"Invalid email format: {partner_data['email']}")
                # Remove invalid email instead of failing
                del partner_data['email']
        
        # Validate VAT format if provided
        if 'vat' in partner_data and partner_data['vat']:
            vat = partner_data['vat'].replace(' ', '').replace('.', '').replace('-', '')
            if not vat.isalnum() or len(vat) < 8:
                _logger.warning(f"Invalid VAT format: {partner_data['vat']}")
                # Remove invalid VAT instead of failing
                del partner_data['vat']
        
        # Set partner type based on name

        partner_data['is_company'] = True
        partner_data['company_type'] = 'company'
        
        # Set country to Italy by default
        italy = self.env['res.country'].search([('code', '=', 'IT')], limit=1)
        if italy:
            partner_data['country_id'] = italy.id
        
        # Process special fields for notes (Num.tel.2 and Fax)
        notes_parts = []
        if 'Num.tel.2' in row and row['Num.tel.2'].strip():
            notes_parts.append(f"Num.tel.2: {row['Num.tel.2'].strip()}")
        
        if 'Fax' in row and row['Fax'].strip():
            notes_parts.append(f"Fax: {row['Fax'].strip()}")
        
        # Add notes if any special fields were found
        if notes_parts:
            partner_data['comment'] = ', '.join(notes_parts)

        
        return partner_data

    def _create_or_update_partner(self, partner_data):
        """
        Create or update partner record
        Returns dict with action info: {'action': 'created'|'updated', 'partner': partner_record, 'vat': vat_value, 'cf': cf_value}
        """
        # Extract VAT and Codice Fiscale before creating partner
        vat_value = partner_data.pop('vat', None)
        cf_value = partner_data.pop('l10n_it_codice_fiscale', None)
        
        try:
            # Check if partner already exists by codice or name
            domain = []
            if 'ref' in partner_data:
                domain.append(('ref', '=', partner_data['ref']))
            else:
                domain.append(('name', '=', partner_data['name']))
            
            existing_partner = self.env['res.partner'].search(domain, limit=1)
            
            if existing_partner:
                # Update existing partner without VAT and CF
                try:
                    existing_partner.write(partner_data)
                except Exception as e:
                    _logger.error(f"Error updating partner: {str(e)}")
                    self.env.cr.rollback()

                _logger.info(f"Updated partner: {partner_data.get('name')} (ID: {existing_partner.id})")
                result = {'action': 'updated', 'partner': existing_partner, 'vat': vat_value, 'cf': cf_value}
            else:
                # Create new partner without VAT and CF
                new_partner = self.env['res.partner'].create(partner_data)
                _logger.info(f"Created new partner: {partner_data.get('name')} (ID: {new_partner.id})")
                result = {'action': 'created', 'partner': new_partner, 'vat': vat_value, 'cf': cf_value}

                self._update_partner_vat_cf_sql(result['partner'].id, vat_value, cf_value)
                
            
            return result
            
        except Exception as e:
            error_msg = f"Error creating/updating partner {partner_data.get('name', 'N/A')}: {str(e)}"
            
            # Log error to ir.logging
            self._log_import_error(
                error_type="Partner Create/Update Error",
                message=error_msg,
                details=f"Partner data: {partner_data}",
                import_type="partners"
            )
            
            _logger.error(error_msg)
            raise ValidationError(f"Error creating/updating partner: {str(e)}")
    
    def _update_partner_vat_cf_sql(self, partner_id, vat_value, cf_value):
        """
        Update partner VAT and Codice Fiscale using direct SQL to bypass validation.
        If an error occurs, append the error message to the partner's 'comment' field (text), using SQL.
        Handles VAT and CF separately, so if both are present and both fail, both errors are logged.
        The comment is always appended (not overwritten).
        """
        # Try to update VAT first, then CF, so both can be attempted and errors logged individually
        self.env.cr.commit()
        if vat_value:
            try:
                sql = "UPDATE res_partner SET vat = %s, write_date = NOW() WHERE id = %s"
                self.env.cr.execute(sql, (vat_value, partner_id))
                self.env.cr.commit()
                _logger.info(f"Updated partner {partner_id} with SQL - VAT: {vat_value}")
            except Exception as e:
                _logger.warning(f"Failed to update VAT for partner {partner_id} with SQL: {str(e)}")
                self.env.cr.rollback()
                # Log error in comment field (append, not overwrite)
                try:
                    comment_sql = """
                        UPDATE res_partner
                        SET comment = 
                            CASE 
                                WHEN comment IS NULL OR comment = '' THEN %s
                                ELSE comment || %s
                            END,
                            write_date = NOW()
                        WHERE id = %s
                    """
                    error_msg = f"Partita IVA non valida: {str(e)}\n"
                    self.env.cr.execute(comment_sql, (error_msg, error_msg, partner_id))
                except Exception as e2:
                    _logger.warning(f"Failed to log VAT error in comment for partner {partner_id}: {str(e2)}")
        if cf_value:
            try:
                sql = "UPDATE res_partner SET l10n_it_codice_fiscale = %s, write_date = NOW() WHERE id = %s"
                self.env.cr.execute(sql, (cf_value, partner_id))
                self.env.cr.commit()
                _logger.info(f"Updated partner {partner_id} with SQL - CF: {cf_value}")
            except Exception as e:
                _logger.warning(f"Failed to update Codice Fiscale for partner {partner_id} with SQL: {str(e)}")
                # Log error in comment field (append, not overwrite)
                self.env.cr.rollback()
                try:
                    comment_sql = """
                        UPDATE res_partner
                        SET comment = 
                            CASE 
                                WHEN comment IS NULL OR comment = '' THEN %s
                                ELSE comment || %s
                            END,
                            write_date = NOW()
                        WHERE id = %s
                    """
                    error_msg = f"Codice Fiscale non valido: {cf_value}\n"
                    self.env.cr.execute(comment_sql, (error_msg, error_msg, partner_id))
                except Exception as e2:
                    _logger.warning(f"Failed to log CF error in comment for partner {partner_id}: {str(e2)}")

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
        _logger.info(f"Preparing project data from row: {row}")
        
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
                    # Mappa standardizzata per le fasi progetto
                    # Se esistono già: Da fare, In corso, Completato, Annullato
                    # Se arriva per esempio "In Progress" o "Closed", mappali su quelli esistenti
                    standard_stage_map = {
                        'DA FARE': 'Da fare',
                        'TO DO': 'Da fare',
                        'IN CORSO': 'In corso',
                        'IN PROGRESS': 'In corso',
                        'COMPLETATO': 'Completato',
                        'COMPLETED': 'Completato',
                        'CHIUSO': 'Completato',
                        'ANNULLATO': 'Annullato',
                        'CANCELLED': 'Annullato',
                        'CANCELED': 'Annullato',
                    }
                    stage_key = value.strip().upper()
                    stage_name = standard_stage_map.get(stage_key)
                    if not stage_name:
                        # Se non è nella mappa, usa il valore originale come nome stage
                        stage_name = value.strip()
                        _logger.info(f"Stage '{stage_name}' non presente in mappa, verrà creato come nuovo stage.")
                    # Cerca se esiste già uno stage con questo nome
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
                elif odoo_field in ['date_start', 'date_end', 'date']:
                    # Parse dates
                    try:
                        # Try different date formats, including the specific format from CSV
                        date_formats = [
                            '%d/%m/%Y %H:%M',      # 31/03/2024 1:00
                            '%d/%m/%Y %H:%M:%S',   # 31/03/2024 1:00:00
                            '%d/%m/%Y',            # 31/03/2024
                            '%Y-%m-%d %H:%M:%S',   # 2024-03-31 01:00:00
                            '%Y-%m-%d %H:%M',      # 2024-03-31 01:00
                            '%Y-%m-%d',            # 2024-03-31
                            '%d-%m-%Y %H:%M',      # 31-03-2024 1:00
                            '%d-%m-%Y',            # 31-03-2024
                        ]
                        parsed_date = None
                        
                        for date_format in date_formats:
                            try:
                                parsed_date = datetime.strptime(value, date_format)
                                break
                            except ValueError:
                                continue
                        
                        if parsed_date:
                            # Convert to Odoo datetime format
                            project_data[odoo_field] = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                            _logger.info(f"Successfully parsed date '{value}' as '{project_data[odoo_field]}' for field {odoo_field}")
                        else:
                            _logger.warning(f"Unable to parse date '{value}' for field {odoo_field}. Supported formats: DD/MM/YYYY HH:MM, DD/MM/YYYY, YYYY-MM-DD HH:MM:SS")
                    except Exception as e:
                        _logger.warning(f"Error parsing date '{value}': {str(e)}")
                else:
                    project_data[odoo_field] = value
        
        # Set default values and validate required fields
        if 'name' not in project_data or not project_data['name'].strip():
            error_msg = "Project name (Commessa) is required"
            self._log_import_error(
                error_type="Project Validation Error",
                message=error_msg,
                details=f"Row data: {row}",
                import_type="projects"
            )
            raise ValidationError(error_msg)
        
        # Validate date fields if provided
        for date_field in ['date_start', 'date_end', 'date']:
            if date_field in project_data and project_data[date_field]:
                try:
                    # Validate that the date string is properly formatted for Odoo
                    datetime.strptime(project_data[date_field], '%Y-%m-%d %H:%M:%S')
                    _logger.info(f"Date validation successful for {date_field}: {project_data[date_field]}")
                except ValueError:
                    _logger.warning(f"Invalid date format for {date_field}: {project_data[date_field]}. Expected format: YYYY-MM-DD HH:MM:SS")
                    # Remove invalid date instead of failing
                    del project_data[date_field]
        
        # Set default project type if not specified
        if 'type_dbm' not in project_data:
            project_data['type_dbm'] = 'GENERICO'
        
        _logger.info(f"Final project data prepared: {project_data}")
        return project_data

    def _create_or_update_project(self, project_data):
        """
        Create or update project record
        Returns dict with action info: {'action': 'created'|'updated', 'project': project_record}
        """
        try:
            _logger.info(f"Attempting to create/update project with data: {project_data}")
            
            # Check if project already exists by code or name
            domain = []
            if 'code' in project_data and project_data['code']:
                domain.append(('code', '=', project_data['code']))
                _logger.info(f"Searching for existing project by code: {project_data['code']}")
            else:
                domain.append(('name', '=', project_data['name']))
                _logger.info(f"Searching for existing project by name: {project_data['name']}")
            
            existing_project = self.env['project.project'].search(domain, limit=1)
            _logger.info(f"Found existing project: {existing_project}")
            
            if existing_project:
                # Update existing project
                _logger.info(f"Updating existing project ID: {existing_project.id}")
                existing_project.write(project_data)
                existing_project.user_id = self.user_id.id
                _logger.info(f"Successfully updated project: {project_data.get('name')} (ID: {existing_project.id})")
                return {'action': 'updated', 'project': existing_project}
            else:
                # Create new project
                _logger.info(f"Creating new project with data: {project_data}")
                new_project = self.env['project.project'].create(project_data)
                _logger.info(f"Successfully created new project: {project_data.get('name')} (ID: {new_project.id})")

                new_project.user_id = self.user_id.id
                
                # Verify the project was actually created
                if new_project.exists():
                    _logger.info(f"Project creation verified - ID: {new_project.id}, Name: {new_project.name}")
                else:
                    _logger.error("Project creation failed - record does not exist after creation")
                    raise ValidationError("Project creation failed - record does not exist after creation")
                
                return {'action': 'created', 'project': new_project}
        except Exception as e:
            error_msg = f"Error creating/updating project {project_data.get('name', 'N/A')}: {str(e)}"
            
            # Log error to ir.logging
            self._log_import_error(
                error_type="Project Create/Update Error",
                message=error_msg,
                details=f"Project data: {project_data}",
                import_type="projects"
            )
            
            _logger.error(error_msg)
            raise ValidationError(f"Error creating/updating project: {str(e)}")

    def _import_activities(self, file_data):
        """
        Import activities from CSV file using project.task
        """
        # Start a new transaction for this import
        self.env.cr.commit()
        
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
                'Attività': 'name',
                'Azienda': 'partner_id',
                'In carico a': 'user_ids',
                'Data': 'planned_date_start',
                'Fatta/da fare': 'stage_id',
                'Macro tipo': 'tag_ids',
                'Commessa': 'project_id',
                'Descrizione attività': 'description',
                'Tipo attività': 'tag_ids',
                'Referente': 'partner_ref_id',
            }
            
            created_count = 0
            updated_count = 0
            error_count = 0
            errors = []
            created_activities = []
            updated_activities = []
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 if header exists
                try:
                    activity_data = self._prepare_activity_data(row, field_mapping)
                    if activity_data:
                        result = self._create_or_update_activity(activity_data)
                        if result['action'] == 'created':
                            created_count += 1
                            created_activities.append(f"{activity_data.get('name', 'N/A')} (Commessa: {activity_data.get('project_code', 'N/A')})")
                            _logger.info(f"{row_num} - Created activity: {activity_data.get('name', 'N/A')}")
                        elif result['action'] == 'updated':
                            updated_count += 1
                            updated_activities.append(f"{activity_data.get('name', 'N/A')} (Commessa: {activity_data.get('project_code', 'N/A')})")
                    
                except Exception as e:
                    error_count += 1
                    activity_name = row.get('Attività', 'N/A')
                    project_code = row.get('Commessa', 'N/A')
                    error_msg = f"Row {row_num} - {activity_name} (Commessa: {project_code}): {str(e)}"
                    errors.append(error_msg)
                    
                    # Log error to ir.logging
                    self._log_import_error(
                        error_type="Activity Import Row Error",
                        message=error_msg,
                        details=f"Activity: {activity_name}, Project Code: {project_code}",
                        row_number=row_num,
                        import_type="activities"
                    )
                    
                    _logger.error(f"Error importing activity at row {row_num}: {str(e)}")
            
            # Update note with detailed results
            result_message = f"Import attività completato:\n"
            result_message += f"- Attività create: {created_count}\n"
            result_message += f"- Attività aggiornate: {updated_count}\n"
            result_message += f"- Errori: {error_count}\n"
            
            # Show created activities
            if created_activities:
                result_message += f"\nAttività create:\n"
                for activity in created_activities[:10]:  # Show first 10
                    result_message += f"- {activity}\n"
                if len(created_activities) > 10:
                    result_message += f"... e altre {len(created_activities) - 10} attività create\n"
            
            # Show updated activities
            if updated_activities:
                result_message += f"\nAttività aggiornate:\n"
                for activity in updated_activities[:10]:  # Show first 10
                    result_message += f"- {activity}\n"
                if len(updated_activities) > 10:
                    result_message += f"... e altre {len(updated_activities) - 10} attività aggiornate\n"
            
            # Show errors
            if errors:
                result_message += f"\nErrori riscontrati:\n"
                for error in errors[:10]:  # Show first 10 errors
                    result_message += f"- {error}\n"
                if len(errors) > 10:
                    result_message += f"... e altri {len(errors) - 10} errori\n"
            
            self.note = result_message
            
            # Prepare notification message
            notification_msg = f"Create: {created_count}, Aggiornate: {updated_count}"
            if error_count > 0:
                notification_msg += f", Errori: {error_count}"
            
            self.env.cr.commit()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Import Attività Completato',
                    'message': notification_msg,
                    'type': 'success' if error_count == 0 else 'warning',
                }
            }
            
        except Exception as e:
            # Rollback the entire transaction on critical error
            error_msg = f"Error processing file: {str(e)}"
            
            # Log critical error to ir.logging
            self._log_import_error(
                error_type="Activity Import Critical Error",
                message=error_msg,
                details=f"File processing failed completely",
                import_type="activities"
            )
            
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _prepare_activity_data(self, row, field_mapping):
        """
        Prepare activity data from CSV row
        """
        activity_data = {}
        #_logger.info(f"Preparing activity data from row: {row}")

        tag_names = {i.name:i.id for i in self.env['project.tags'].search([])}
        stage_names = {i.name:i.id for i in self.env['project.task.type'].search([])}
        user_names = {i.name:i.id for i in self.env['res.users'].search([])}
        
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip():
                value = row[csv_field].strip()
                
                # Special handling for specific fields
                if odoo_field == 'partner_id':
                    # Find company by name
                    company = self.env['res.partner'].search([
                        ('name', 'ilike', value),
                    ], limit=1)
                    if company:
                        activity_data[odoo_field] = company.id
                    else:
                        _logger.warning(f"Company '{value}' not found")
                elif odoo_field == 'partner_ref_id':
                    # Find person by name
                    person = self.env['res.partner'].search([
                        ('name', 'ilike', value),
                    ], limit=1)
                    if person:
                        activity_data[odoo_field] = person.id
                    else:
                        _logger.warning(f"Person '{value}' not found")
                elif odoo_field == 'user_ids':
                    # Set only the user found, removing others
                    user = user_names.get(value, False)
                    if user:
                        activity_data[odoo_field] = [(6, 0, [user])]
                    else:
                        # Use current user if not found
                        activity_data[odoo_field] = [(6, 0, [self.env.user.id])]
                elif odoo_field == 'stage_id':
                    # Map stage names to project.task.stage
                    stage_mapping = {
                        'ANNULLATA': 'Annullata',
                        'TO DO': 'Attività da fare',
                        'COMPLETED': 'Attività fatta',
                    }
                    stage_key = value.strip().upper()
                    stage_name = stage_mapping.get(stage_key, value.strip())
                    
                    # Find stage in project.task.stage
                    stage = stage_names.get(stage_name, False)
                    if not stage:
                        # Use default stage if not found
                        stage = self.env['project.task.type'].create({
                            'name': stage_name
                        }).id
                        stage_names[stage_name] = stage
                        #_logger.warning(f"Stage '{stage_name}' not found, created new stage")
                    if stage:
                        activity_data[odoo_field] = stage
                        if stage_key == 'ATTIVITÀ FATTA':
                            activity_data['state'] = '1_done'
                elif odoo_field == 'tag_ids':
                    if value:
                        tag_id = tag_names.get(value, False)
                        if not tag_id:
                            tag_id = self.env['project.tags'].create({
                                'name': value
                            }).id
                            _logger.warning(f"Tag '{value}' not found, created new tag")
                            tag_names[value] = tag_id
                        if tag_id not in [i[1] for i in activity_data.get(odoo_field, [])]:
                            if activity_data.get(odoo_field, []):
                                activity_data[odoo_field].append((4, tag_id))
                            else:
                                activity_data[odoo_field] = [(4, tag_id)]
                elif odoo_field == 'planned_date_start':
                    # Parse dates and convert to UTC to avoid timezone issues
                    try:
                        import pytz
                        date_formats = [
                            '%d/%m/%Y %H:%M',      # 31/03/2024 1:00
                            '%d/%m/%Y %H:%M:%S',   # 31/03/2024 1:00:00
                            '%d/%m/%Y',            # 31/03/2024
                            '%Y-%m-%d %H:%M:%S',   # 2024-03-31 01:00:00
                            '%Y-%m-%d %H:%M',      # 2024-03-31 01:00
                            '%Y-%m-%d',            # 2024-03-31
                            '%d-%m-%Y %H:%M',      # 31-03-2024 1:00
                            '%d-%m-%Y',            # 31-03-2024
                        ]
                        parsed_date = None

                        for date_format in date_formats:
                            try:
                                parsed_date = datetime.strptime(value, date_format)
                                break
                            except ValueError:
                                continue

                        if parsed_date:
                            # Get user's timezone or default to UTC
                            user_tz = self.env.user.tz or 'UTC'
                            local_tz = pytz.timezone(user_tz)
                            # If the parsed date has no time, set to 00:00
                            if parsed_date.hour == 0 and parsed_date.minute == 0 and parsed_date.second == 0 and ('%H' not in date_format):
                                parsed_date = parsed_date.replace(hour=0, minute=0, second=0)
                            # Localize and convert to UTC
                            localized_date = local_tz.localize(parsed_date)
                            utc_date = localized_date.astimezone(pytz.utc)
                            activity_data[odoo_field] = utc_date.strftime('%Y-%m-%d %H:%M:%S')
                            #_logger.info(f"Successfully parsed date '{value}' as '{activity_data[odoo_field]}' (UTC, user tz: {user_tz})")
                        else:
                            _logger.warning(f"Unable to parse date '{value}'. Supported formats: DD/MM/YYYY HH:MM, DD/MM/YYYY, YYYY-MM-DD HH:MM:SS")
                    except Exception as e:
                        _logger.warning(f"Error parsing date '{value}': {str(e)}")
                elif odoo_field == 'project_id':
                    # Extract the project code by removing the suffix (e.g., "000001-24" from "PROJECT_NAME-000001-24")
                    project_code = value
                    if '-' in project_code:
                        # Split by '-' and take all parts except the last one (which is the suffix)
                        code_parts = project_code.split('-')
                        if len(code_parts) > 1:
                            # Remove the last part (suffix like "000001-24")
                            clean_project_code = '-'.join(code_parts[:-1])
                        else:
                            clean_project_code = project_code
                    else:
                        clean_project_code = project_code
                    if clean_project_code:
                        project = self.env['project.project'].search([('code', '=', clean_project_code.strip())], limit=1)
                        if project:
                            activity_data[odoo_field] = project.id
                        else:
                            _logger.warning(f"Project '{clean_project_code}' not found")
                elif odoo_field == 'name':
                    activity_data[odoo_field] = value.strip()
                elif odoo_field == 'planned_hours':
                    # Parse time duration (skip for now as requested)
                    continue
                else:
                    activity_data[odoo_field] = value
        
        # Set default values and validate required fields
        if 'name' not in activity_data or not activity_data['name'].strip():
            error_msg = "Activity name (Attività) is required"
            self._log_import_error(
                error_type="Activity Validation Error",
                message=error_msg,
                details=f"Row data: {row}",
                import_type="activities"
            )
            raise ValidationError(error_msg)
        
        _logger.info(f"Final activity data prepared: {activity_data}")
        return activity_data

    def _create_or_update_activity(self, activity_data):
        """
        Create or update activity record using project.task
        Returns dict with action info: {'action': 'created'|'updated', 'activity': activity_record}
        """
        try:
            _logger.info(f"Attempting to create/update activity with data: {activity_data}")
            
            # Check if activity already exists by name and project_code
            domain = [('name', '=', activity_data['name'])]
            if 'project_id' in activity_data and activity_data['project_id']:
                # Extract the project code by removing the suffix (e.g., "000001-24" from "PROJECT_NAME-000001-24")
                               
                project = activity_data['project_id']
                if project:
                    domain.append(('project_id', '=', project))
                else:
                    _logger.warning(f"Project '{activity_data['project_id']}' not found")
            
            #existing_activity = self.env['project.task'].search(domain, limit=1)
            existing_activity = False
            _logger.info(f"Found existing activity: {existing_activity}")
            
            if existing_activity:
                # Update existing activity
                _logger.info(f"Updating existing activity ID: {existing_activity.id}")
                existing_activity.write(activity_data)
                _logger.info(f"Successfully updated activity: {activity_data.get('name')} (ID: {existing_activity.id})")
                return {'action': 'updated', 'activity': existing_activity}
            else:
                # Create new activity
                _logger.info(f"Creating new activity with data: {activity_data}")
                new_activity = self.env['project.task'].create(activity_data)
                if self.user_id and not new_activity.user_ids:
                    new_activity.user_ids = [(6, 0, [self.user_id.id])]
                _logger.info(f"Successfully created new activity: {activity_data.get('name')} (ID: {new_activity.id})")
                
                # Verify the activity was actually created
                if new_activity.exists():
                    _logger.info(f"Activity creation verified - ID: {new_activity.id}, Name: {new_activity.name}")
                else:
                    _logger.error("Activity creation failed - record does not exist after creation")
                    raise ValidationError("Activity creation failed - record does not exist after creation")
                
                return {'action': 'created', 'activity': new_activity}
        except Exception as e:
            error_msg = f"Error creating/updating activity {activity_data.get('name', 'N/A')}: {str(e)}"
            
            # Log error to ir.logging
            self._log_import_error(
                error_type="Activity Create/Update Error",
                message=error_msg,
                details=f"Activity data: {activity_data}",
                import_type="activities"
            )
            
            _logger.error(error_msg)
            raise ValidationError(f"Error creating/updating activity: {str(e)}")

    def _import_helpdesk_tickets(self, file_data):
        """
        Import helpdesk tickets from CSV file
        """
        # Start a new transaction for this import
        self.env.cr.commit()
        
        # Check if helpdesk module is installed
        helpdesk_module = self.env['ir.module.module'].search([('name', '=', 'helpdesk_mgmt'), ('state', '=', 'installed')])
        if not helpdesk_module:
            raise UserError("The 'helpdesk_mgmt' module is not installed. Please install it first to import helpdesk tickets.")
        
        _logger.info("Helpdesk module is installed, proceeding with import")
        
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
                'Oggetto': 'name',
                'Codice': 'number',
                'Proprietario': 'user_id',
                'Cliente': 'partner_id',
                'Descrizione': 'description',
                'Stato': 'stage_id',
                'Data inizio effettiva': 'assigned_date',
                'Data creazione': 'create_date',
                'Data Pianificazione': 'planned_date',
            }
            
            created_count = 0
            updated_count = 0
            error_count = 0
            errors = []
            created_tickets = []
            updated_tickets = []
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 if header exists
                try:
                    ticket_data = self._prepare_helpdesk_ticket_data(row, field_mapping)
                    if ticket_data:
                        result = self._create_or_update_helpdesk_ticket(ticket_data)
                        if result['action'] == 'created':
                            created_count += 1
                            created_tickets.append(f"{ticket_data.get('name', 'N/A')} (Codice: {ticket_data.get('number', 'N/A')})")
                        elif result['action'] == 'updated':
                            updated_count += 1
                            updated_tickets.append(f"{ticket_data.get('name', 'N/A')} (Codice: {ticket_data.get('number', 'N/A')})")
                    
                except Exception as e:
                    error_count += 1
                    ticket_name = row.get('Oggetto', 'N/A')
                    ticket_code = row.get('Codice', 'N/A')
                    error_msg = f"Row {row_num} - {ticket_name} (Codice: {ticket_code}): {str(e)}"
                    errors.append(error_msg)
                    
                    # Log error to ir.logging
                    self._log_import_error(
                        error_type="Helpdesk Ticket Import Row Error",
                        message=error_msg,
                        details=f"Ticket: {ticket_name}, Code: {ticket_code}",
                        row_number=row_num,
                        import_type="helpdesk_tickets"
                    )
                    
                    _logger.error(f"Error importing helpdesk ticket at row {row_num}: {str(e)}")
            
            # Update note with detailed results
            self.env.cr.commit()
            result_message = f"Import ticket helpdesk completato:\n"
            result_message += f"- Ticket creati: {created_count}\n"
            result_message += f"- Ticket aggiornati: {updated_count}\n"
            result_message += f"- Errori: {error_count}\n"
            
            # Show created tickets
            if created_tickets:
                result_message += f"\nTicket creati:\n"
                for ticket in created_tickets[:10]:  # Show first 10
                    result_message += f"- {ticket}\n"
                if len(created_tickets) > 10:
                    result_message += f"... e altri {len(created_tickets) - 10} ticket creati\n"
            
            # Show updated tickets
            if updated_tickets:
                result_message += f"\nTicket aggiornati:\n"
                for ticket in updated_tickets[:10]:  # Show first 10
                    result_message += f"- {ticket}\n"
                if len(updated_tickets) > 10:
                    result_message += f"... e altri {len(updated_tickets) - 10} ticket aggiornati\n"
            
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
            
            self.env.cr.commit()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Import Ticket Helpdesk Completato',
                    'message': notification_msg,
                    'type': 'success' if error_count == 0 else 'warning',
                }
            }
            
        except Exception as e:
            # Rollback the entire transaction on critical error
            error_msg = f"Error processing file: {str(e)}"
            
            # Log critical error to ir.logging
            self._log_import_error(
                error_type="Helpdesk Ticket Import Critical Error",
                message=error_msg,
                details=f"File processing failed completely",
                import_type="helpdesk_tickets"
            )
            
            _logger.error(error_msg)
            raise UserError(error_msg)

    def _prepare_helpdesk_ticket_data(self, row, field_mapping):
        """
        Prepare helpdesk ticket data from CSV row
        """
        ticket_data = {}
        _logger.info(f"Preparing helpdesk ticket data from row: {row}")
        
        for csv_field, odoo_field in field_mapping.items():
            if csv_field in row and row[csv_field].strip():
                value = row[csv_field].strip()
                
                # Special handling for specific fields
                if odoo_field == 'user_id':
                    # Find user by name
                    if self.user_id:
                        ticket_data[odoo_field] = self.user_id.id
                    else:
                        user = self.env['res.users'].search([
                            ('name', 'ilike', value)
                        ], limit=1)
                        if user:
                            ticket_data[odoo_field] = user.id
                        else:
                            _logger.warning(f"User '{value}' not found, using current user")
                            ticket_data[odoo_field] = self.env.user.id
                        
                elif odoo_field == 'partner_id':
                    # Find partner by name
                    partner = self.env['res.partner'].search([
                        ('name', 'ilike', value)
                    ], limit=1)
                    if partner:
                        ticket_data[odoo_field] = partner.id
                        ticket_data['partner_name'] = value
                    else:
                        _logger.warning(f"Partner '{value}' not found")
                        
                elif odoo_field == 'stage_id':
                    # Map stage names to helpdesk.ticket.stage
                    stage_mapping = {
                        'APERTO': 'Nuovo',
                        'IN CORSO': 'In corso', 
                        'IN ATTESA': 'In attesa',
                        'CHIUSO - OK': 'Fatto',
                        'CHIUSO - KO': 'Respinto',
                        'ANNULLATO': 'Annullato',
                        'PIANIFICAZIONE': 'Nuovo',  # Map to Nuovo for planning stage
                        'SOSPESO': 'In attesa',  # Map to In attesa for suspended
                    }
                    
                    stage_key = value.strip().upper()
                    stage_name = stage_mapping.get(stage_key, value.strip())
                    
                    # Find or create stage
                    stage = self.env['helpdesk.ticket.stage'].search([('name', '=', stage_name)], limit=1)
                    if not stage:
                        # Create new stage if not found
                        stage = self.env['helpdesk.ticket.stage'].create({
                            'name': stage_name,
                            'sequence': 10,
                            'closed': True if 'CHIUSO' in stage_key else False,
                            'unattended': True if 'ATTESA' in stage_key or 'SOSPESO' in stage_key else False,
                        })
                        _logger.info(f"Created new helpdesk stage: {stage_name}")
                    
                    ticket_data[odoo_field] = stage.id
                    
                elif odoo_field in ['assigned_date', 'create_date', 'planned_date']:
                    # Parse dates
                    try:
                        date_formats = [
                            '%d/%m/%Y %H:%M',      # 16/09/2025 9:30
                            '%d/%m/%Y %H:%M:%S',   # 16/09/2025 9:30:00
                            '%d/%m/%Y',            # 16/09/2025
                            '%Y-%m-%d %H:%M:%S',   # 2025-09-16 09:30:00
                            '%Y-%m-%d %H:%M',      # 2025-09-16 09:30
                            '%Y-%m-%d',            # 2025-09-16
                            '%d-%m-%Y %H:%M',      # 16-09-2025 9:30
                            '%d-%m-%Y',            # 16-09-2025
                        ]
                        parsed_date = None
                        
                        for date_format in date_formats:
                            try:
                                parsed_date = datetime.strptime(value, date_format)
                                break
                            except ValueError:
                                continue
                        
                        if parsed_date:
                            # Convert to Odoo datetime format
                            ticket_data[odoo_field] = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
                            _logger.info(f"Successfully parsed date '{value}' as '{ticket_data[odoo_field]}' for field {odoo_field}")
                        else:
                            _logger.warning(f"Unable to parse date '{value}' for field {odoo_field}")
                    except Exception as e:
                        _logger.warning(f"Error parsing date '{value}': {str(e)}")
                        
                elif odoo_field == 'description':
                    # Convert plain text to HTML for description field
                    if value:
                        # Escape HTML characters and convert line breaks
                        import html
                        escaped_value = html.escape(value)
                        html_value = escaped_value.replace('\n', '<br/>')
                        ticket_data[odoo_field] = f"<p>{html_value}</p>"
                    else:
                        ticket_data[odoo_field] = "<p></p>"
                        
                else:
                    ticket_data[odoo_field] = value
        
        # Set default values and validate required fields
        if 'name' not in ticket_data or not ticket_data['name'].strip():
            error_msg = "Ticket name (Oggetto) is required"
            self._log_import_error(
                error_type="Helpdesk Ticket Validation Error",
                message=error_msg,
                details=f"Row data: {row}",
                import_type="helpdesk_tickets"
            )
            raise ValidationError(error_msg)
        
        # Set default description if not provided
        if 'description' not in ticket_data:
            ticket_data['description'] = "<p></p>"
        
        # Set default user if not provided
        if 'user_id' not in ticket_data:
            ticket_data['user_id'] = self.env.user.id
            
        # Set default stage if not provided
        if 'stage_id' not in ticket_data:
            default_stage = self.env['helpdesk.ticket.stage'].search([('name', '=', 'Nuovo')], limit=1)
            if not default_stage:
                default_stage = self.env['helpdesk.ticket.stage'].create({
                    'name': 'Nuovo',
                    'sequence': 10,
                    'closed': False,
                    'unattended': False,
                })
            ticket_data['stage_id'] = default_stage.id
        
        _logger.info(f"Final helpdesk ticket data prepared: {ticket_data}")
        ticket_data.update({'active': True, })
        return ticket_data

    def _create_or_update_helpdesk_ticket(self, ticket_data):
        """
        Create or update helpdesk ticket record using SQL
        Returns dict with action info: {'action': 'created'|'updated', 'ticket': ticket_record}
        """
        try:
            _logger.info(f"Attempting to create/update helpdesk ticket with data: {ticket_data}")
            
            # Check if ticket already exists by number or name
            existing_ticket_id = None
            if 'number' in ticket_data and ticket_data['number']:
                _logger.info(f"Searching for existing ticket by number: {ticket_data['number']}")
                self.env.cr.execute(
                    "SELECT id FROM helpdesk_ticket WHERE number = %s LIMIT 1",
                    (ticket_data['number'],)
                )
                result = self.env.cr.fetchone()
                if result:
                    existing_ticket_id = result[0]
            else:
                _logger.info(f"Searching for existing ticket by name: {ticket_data['name']}")
                self.env.cr.execute(
                    "SELECT id FROM helpdesk_ticket WHERE name = %s LIMIT 1",
                    (ticket_data['name'],)
                )
                result = self.env.cr.fetchone()
                if result:
                    existing_ticket_id = result[0]
            
            if existing_ticket_id:
                # Update existing ticket using SQL
                _logger.info(f"Updating existing ticket ID: {existing_ticket_id}")
                self._update_helpdesk_ticket_sql(existing_ticket_id, ticket_data)
                _logger.info(f"Successfully updated ticket: {ticket_data.get('name')} (ID: {existing_ticket_id})")
                return {'action': 'updated', 'ticket': self.env['helpdesk.ticket'].browse(existing_ticket_id)}
            else:
                # Create new ticket using SQL
                _logger.info(f"Creating new ticket with data: {ticket_data}")
                new_ticket_id = self._create_helpdesk_ticket_sql(ticket_data)
                _logger.info(f"Successfully created new ticket: {ticket_data.get('name')} (ID: {new_ticket_id})")
                return {'action': 'created', 'ticket': self.env['helpdesk.ticket'].browse(new_ticket_id)}
                
        except Exception as e:
            error_msg = f"Error creating/updating helpdesk ticket {ticket_data.get('name', 'N/A')}: {str(e)}"
            
            # Log error to ir.logging
            self._log_import_error(
                error_type="Helpdesk Ticket Create/Update Error",
                message=error_msg,
                details=f"Ticket data: {ticket_data}",
                import_type="helpdesk_tickets"
            )
            
            _logger.error(error_msg)
            raise ValidationError(f"Error creating/updating helpdesk ticket: {str(e)}")

    def _create_helpdesk_ticket_sql(self, ticket_data):
        """
        Create helpdesk ticket using direct SQL
        """
        try:
            # Prepare SQL fields and values
            field_names = []
            values = []
            placeholders = []
            
            # Required fields
            field_names.append('name')
            values.append(ticket_data.get('name', ''))
            placeholders.append('%s')
            
            field_names.append('description')
            values.append(ticket_data.get('description', '<p></p>'))
            placeholders.append('%s')
            
            field_names.append('user_id')
            values.append(ticket_data.get('user_id', self.env.user.id))
            placeholders.append('%s')
            
            field_names.append('stage_id')
            values.append(ticket_data.get('stage_id'))
            placeholders.append('%s')
            
            field_names.append('active')
            values.append(ticket_data.get('active', True))
            placeholders.append('%s')
            
            field_names.append('company_id')
            values.append(ticket_data.get('company_id', self.env.company.id))
            placeholders.append('%s')
            
            field_names.append('create_uid')
            values.append(self.env.user.id)
            placeholders.append('%s')
            
            field_names.append('create_date')
            values.append(datetime.now())
            placeholders.append('%s')
            
            field_names.append('write_uid')
            values.append(self.env.user.id)
            placeholders.append('%s')
            
            field_names.append('write_date')
            values.append(datetime.now())
            placeholders.append('%s')
            
            # Optional fields
            if 'number' in ticket_data and ticket_data['number']:
                field_names.append('number')
                values.append(ticket_data['number'])
                placeholders.append('%s')
            else:
                # Generate sequence number
                seq_number = self._generate_ticket_number()
                field_names.append('number')
                values.append(seq_number)
                placeholders.append('%s')
            
            if 'partner_id' in ticket_data and ticket_data['partner_id']:
                field_names.append('partner_id')
                values.append(ticket_data['partner_id'])
                placeholders.append('%s')
            
            if 'partner_name' in ticket_data and ticket_data['partner_name']:
                field_names.append('partner_name')
                values.append(ticket_data['partner_name'])
                placeholders.append('%s')
            
            if 'assigned_date' in ticket_data and ticket_data['assigned_date']:
                field_names.append('assigned_date')
                values.append(ticket_data['assigned_date'])
                placeholders.append('%s')
            
            if 'closed_date' in ticket_data and ticket_data['closed_date']:
                field_names.append('closed_date')
                values.append(ticket_data['closed_date'])
                placeholders.append('%s')
            
            if 'last_stage_update' in ticket_data and ticket_data['last_stage_update']:
                field_names.append('last_stage_update')
                values.append(ticket_data['last_stage_update'])
                placeholders.append('%s')
            else:
                field_names.append('last_stage_update')
                values.append(datetime.now())
                placeholders.append('%s')
            
            # Build and execute SQL
            fields_str = ', '.join(field_names)
            placeholders_str = ', '.join(placeholders)
            
            sql = f"""
                INSERT INTO helpdesk_ticket ({fields_str})
                VALUES ({placeholders_str})
                RETURNING id
            """
            
            self.env.cr.execute(sql, values)
            ticket_id = self.env.cr.fetchone()[0]
            self.env.cr.commit()
            
            _logger.info(f"Created helpdesk ticket with SQL - ID: {ticket_id}")
            return ticket_id
            
        except Exception as e:
            self.env.cr.rollback()
            _logger.error(f"Error creating helpdesk ticket with SQL: {str(e)}")
            raise

    def _update_helpdesk_ticket_sql(self, ticket_id, ticket_data):
        """
        Update helpdesk ticket using direct SQL
        """
        try:
            # Prepare SQL fields and values for update
            set_clauses = []
            values = []
            
            # Update fields
            if 'name' in ticket_data:
                set_clauses.append('name = %s')
                values.append(ticket_data['name'])
            
            if 'description' in ticket_data:
                set_clauses.append('description = %s')
                values.append(ticket_data['description'])
            
            if 'user_id' in ticket_data:
                set_clauses.append('user_id = %s')
                values.append(ticket_data['user_id'])
            
            if 'stage_id' in ticket_data:
                set_clauses.append('stage_id = %s')
                values.append(ticket_data['stage_id'])
            
            if 'partner_id' in ticket_data:
                set_clauses.append('partner_id = %s')
                values.append(ticket_data['partner_id'])
            
            if 'partner_name' in ticket_data:
                set_clauses.append('partner_name = %s')
                values.append(ticket_data['partner_name'])
            
            if 'assigned_date' in ticket_data:
                set_clauses.append('assigned_date = %s')
                values.append(ticket_data['assigned_date'])
            
            if 'closed_date' in ticket_data:
                set_clauses.append('closed_date = %s')
                values.append(ticket_data['closed_date'])
            
            if 'last_stage_update' in ticket_data:
                set_clauses.append('last_stage_update = %s')
                values.append(ticket_data['last_stage_update'])
            
            if 'active' in ticket_data:
                set_clauses.append('active = %s')
                values.append(ticket_data['active'])
            
            # Always update write fields
            set_clauses.append('write_uid = %s')
            values.append(self.env.user.id)
            
            set_clauses.append('write_date = %s')
            values.append(datetime.now())
            
            # Add ticket_id to values
            values.append(ticket_id)
            
            # Build and execute SQL
            set_clause = ', '.join(set_clauses)
            sql = f"UPDATE helpdesk_ticket SET {set_clause} WHERE id = %s"
            
            self.env.cr.execute(sql, values)
            self.env.cr.commit()
            
            _logger.info(f"Updated helpdesk ticket with SQL - ID: {ticket_id}")
            
        except Exception as e:
            self.env.cr.rollback()
            _logger.error(f"Error updating helpdesk ticket with SQL: {str(e)}")
            raise

    def _generate_ticket_number(self):
        """
        Generate ticket number using sequence
        """
        try:
            seq = self.env['ir.sequence']
            if hasattr(self, 'company_id') and self.company_id:
                seq = seq.with_company(self.company_id.id)
            return seq.next_by_code('helpdesk.ticket.sequence') or '/'
        except Exception as e:
            _logger.warning(f"Error generating ticket number: {str(e)}")
            return f"TICKET-{int(datetime.now().timestamp())}"