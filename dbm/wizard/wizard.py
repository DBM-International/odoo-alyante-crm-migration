 
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
        elif import_type == "project":
            return self._import_projects(file_data)
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
                _logger.info(f"Successfully updated project: {project_data.get('name')} (ID: {existing_project.id})")
                return {'action': 'updated', 'project': existing_project}
            else:
                # Create new project
                _logger.info(f"Creating new project with data: {project_data}")
                new_project = self.env['project.project'].create(project_data)
                _logger.info(f"Successfully created new project: {project_data.get('name')} (ID: {new_project.id})")
                
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