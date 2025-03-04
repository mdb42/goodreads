from PyQt6.QtWidgets import (
    QWidget, QSplitter, QVBoxLayout, QTreeWidget, QTreeWidgetItem, 
    QTableView, QHeaderView, QLabel, QHBoxLayout, QLineEdit, QSpinBox
)
from PyQt6.QtCore import Qt, QAbstractTableModel, QVariant
from PyQt6.QtGui import QFont

import qtawesome as qta
import logging
import sqlite3

# Set up module-level logging.
logger = logging.getLogger(__name__)

class SqlTableModel(QAbstractTableModel):
    """
    A Qt table model that displays data from an SQLite table.

    This model retrieves the table structure and data from the specified SQLite table
    and makes it available for display in a QTableView.
    """
    
    def __init__(self, db_path, table_name, parent=None):
        """
        Initialize the model by connecting to the database and loading data.

        Args:
            db_path (str): Path to the SQLite database file.
            table_name (str): Name of the table to display.
            parent (QObject, optional): Parent object.
        """
        super().__init__(parent)
        self.logger = logging.getLogger(__name__)
        self.conn = sqlite3.connect(db_path)
        self.table_name = table_name
        self.headers = []
        self.data_types = []
        # Renaming the attribute to avoid conflict with the method data()
        self._data = []
        self.load_data()
    
    def load_data(self, limit=1000, where_clause=None):
        """
        Load data from the SQLite table with optional filtering.

        Args:
            limit (int): Maximum number of rows to retrieve.
            where_clause (str, optional): An SQL WHERE clause to filter the data.

        Returns:
            bool: True if data is loaded successfully, otherwise False.
        
        Notes:
            - Retrieves the table's column names and data types via PRAGMA.
            - Emits layoutChanged signal after loading to update any connected views.
        """
        try:
            cursor = self.conn.cursor()
            # Retrieve table structure.
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns_info = cursor.fetchall()
            self.headers = [col[1] for col in columns_info]  # Column names.
            self.data_types = [col[2] for col in columns_info]  # Column data types.
            
            # Debug logging: output table information.
            self.logger.debug(f"Table: {self.table_name}, Columns: {self.headers}")
            
            # Build SQL query with optional filtering.
            query = f"SELECT * FROM {self.table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" LIMIT {limit}"
            
            self.logger.debug(f"Executing query: {query}")
            
            cursor.execute(query)
            self._data = cursor.fetchall()
            
            self.logger.debug(f"Fetched {len(self._data)} rows")
            if self._data:
                self.logger.debug(f"Sample row: {self._data[0]}")
            
            # Notify views that the model data has changed.
            self.layoutChanged.emit()
            return True
        except Exception as e:
            self.logger.error(f"Error loading data from {self.table_name}: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def rowCount(self, parent=None):
        """
        Return the number of rows in the model.

        Args:
            parent (QModelIndex, optional): Parent index.

        Returns:
            int: Number of rows.
        """
        return len(self._data)
    
    def columnCount(self, parent=None):
        """
        Return the number of columns in the model.

        Args:
            parent (QModelIndex, optional): Parent index.

        Returns:
            int: Number of columns.
        """
        return len(self.headers)
    
    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        """
        Return the data to be displayed for the given index and role.

        Args:
            index (QModelIndex): The cell index.
            role (int): The role for which the data is requested.

        Returns:
            QVariant: Data for display or an empty QVariant if not applicable.
        """
        if not index.isValid() or not (0 <= index.row() < len(self._data)):
            return QVariant()
        
        value = self._data[index.row()][index.column()]
        
        if role == Qt.ItemDataRole.DisplayRole:
            # Format based on data type.
            if value is None:
                return "NULL"
            elif isinstance(value, (int, float)):
                return str(value)
            elif isinstance(value, str) and len(value) > 100:
                return value[:100] + "..."  # Truncate long text.
            return str(value)
            
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            # Align numeric values to the right.
            if isinstance(value, (int, float)):
                return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            return int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        
        return QVariant()
    
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        """
        Return header data for the given section and orientation.

        Args:
            section (int): Section number.
            orientation (Qt.Orientation): Horizontal or vertical.
            role (int): The role for header data.

        Returns:
            QVariant: Header label or an empty QVariant.
        """
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            if 0 <= section < len(self.headers):
                return self.headers[section]
        return QVariant()
    
    def refresh(self):
        """
        Reload the data from the table.

        Returns:
            bool: True if the data is refreshed successfully.
        """
        return self.load_data()
    
    def getColumnDataType(self, column_index):
        """
        Get the SQLite data type for a given column index.

        Args:
            column_index (int): Index of the column.

        Returns:
            str or None: Data type as a string, or None if index is out of range.
        """
        if 0 <= column_index < len(self.data_types):
            return self.data_types[column_index]
        return None


class DataBrowser(QWidget):
    """
    Main widget for browsing database contents.

    This widget includes a navigation tree displaying the database schema and a table view
    for displaying data from selected tables. It also provides simple search and limit controls.
    """
    def __init__(self, db_path, parent=None):
        """
        Initialize the DataBrowser with a given SQLite database.

        Args:
            db_path (str): Path to the SQLite database file.
            parent (QWidget, optional): Parent widget.
        """
        super().__init__(parent)
        self.logger = logging.getLogger(__name__)
        self.db_path = db_path
        self.current_table = None
        self.current_model = None
        
        self.logger.info(f"Initializing DataBrowser with database path: {db_path}")
        
        # Verify database connection and list tables.
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            self.logger.info(f"Successfully connected to database. Found {len(tables)} tables.")
            conn.close()
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            import traceback
            traceback.print_exc()
        
        self.initUI()
        self.loadDatabaseSchema()
    
    def initUI(self):
        """
        Initialize the user interface components.
        
        Layout includes:
            - A navigation tree for browsing database schema.
            - A content area with a table view and controls (search box, limit spinner).
        """
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Create a splitter to separate navigation and content.
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.splitter.setChildrenCollapsible(False)
        
        # --- Left Side: Navigation Tree ---
        self.navWidget = QWidget()
        navLayout = QVBoxLayout(self.navWidget)
        navHeader = QHBoxLayout()
        navLabel = QLabel("Database Explorer")
        navLabel.setStyleSheet("font-weight: bold; font-size: 14px;")
        navHeader.addWidget(navLabel)
        navHeader.addStretch()
        navLayout.addLayout(navHeader)
        
        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.itemClicked.connect(self.onTreeItemClicked)
        navLayout.addWidget(self.tree)
        
        # --- Right Side: Content Area ---
        self.contentWidget = QWidget()
        contentLayout = QVBoxLayout(self.contentWidget)
        
        # Table header with controls.
        self.tableHeader = QHBoxLayout()
        self.tableLabel = QLabel("Select a table to view data")
        self.tableLabel.setStyleSheet("font-weight: bold; font-size: 14px;")
        self.tableHeader.addWidget(self.tableLabel)
        self.tableHeader.addStretch()
        
        # Search box for filtering data.
        self.searchBox = QLineEdit()
        self.searchBox.setPlaceholderText("Search...")
        self.searchBox.setClearButtonEnabled(True)
        self.searchBox.setFixedWidth(200)
        self.searchBox.setEnabled(False)
        self.searchBox.returnPressed.connect(self.onSearch)
        self.tableHeader.addWidget(self.searchBox)
        
        # Limit selector for controlling number of rows.
        limitLayout = QHBoxLayout()
        limitLayout.addWidget(QLabel("Limit:"))
        self.limitBox = QSpinBox()
        self.limitBox.setRange(1, 10000)
        self.limitBox.setValue(1000)
        self.limitBox.setEnabled(False)
        self.limitBox.valueChanged.connect(self.onLimitChanged)
        limitLayout.addWidget(self.limitBox)
        self.tableHeader.addLayout(limitLayout)
        
        contentLayout.addLayout(self.tableHeader)
        
        # Table view for displaying query results.
        self.tableView = QTableView()
        self.tableView.setSortingEnabled(False)  # Sorting is disabled for now.
        self.tableView.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tableView.verticalHeader().setVisible(True)
        contentLayout.addWidget(self.tableView)
        
        # Add navigation and content to the splitter.
        self.splitter.addWidget(self.navWidget)
        self.splitter.addWidget(self.contentWidget)
        self.splitter.setSizes([200, 600])
        
        layout.addWidget(self.splitter)

    def loadDatabaseSchema(self):
        """
        Load the database schema into the navigation tree.

        Retrieves table and column information from the SQLite database and populates the tree.
        """
        self.tree.clear()
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Create a top-level item for tables.
            tables_item = QTreeWidgetItem(self.tree, ["Tables"])
            tables_item.setIcon(0, qta.icon('fa5s.table'))
            tables_item.setFont(0, QFont(self.font().family(), self.font().pointSize(), QFont.Weight.Bold))
            
            # Retrieve the list of tables.
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            self.logger.info(f"Found {len(tables)} tables in database")
            
            for table in tables:
                table_name = table[0]
                # Skip SQLite internal tables.
                if table_name.startswith('sqlite_'):
                    continue
                    
                table_item = QTreeWidgetItem(tables_item, [table_name])
                table_item.setIcon(0, qta.icon('fa5s.th-list'))
                table_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'table', 'name': table_name})
                
                # Retrieve column information for each table.
                try:
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = cursor.fetchall()
                    self.logger.info(f"Table {table_name} has {len(columns)} columns")
                    
                    for col in columns:
                        col_name = col[1]
                        col_type = col[2]
                        is_pk = col[5] == 1  # Primary key flag.
                        
                        # Display column name with type; append a key icon if it's a primary key.
                        col_text = f"{col_name} ({col_type})"
                        if is_pk:
                            col_text += " 🔑"
                            
                        col_item = QTreeWidgetItem(table_item, [col_text])
                        col_item.setIcon(0, qta.icon('fa5s.columns'))
                        col_item.setData(0, Qt.ItemDataRole.UserRole, 
                                         {'type': 'column', 'name': col_name, 'table': table_name, 'data_type': col_type})
                except Exception as e:
                    self.logger.error(f"Error loading columns for table {table_name}: {e}")
            
            tables_item.setExpanded(True)
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error loading database schema: {e}")
            import traceback
            traceback.print_exc()

    def onTreeItemClicked(self, item, column):
        """
        Handle clicks on items in the navigation tree.

        If a table item is clicked, its data is loaded into the table view.

        Args:
            item (QTreeWidgetItem): The clicked item.
            column (int): The column index.
        """
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if data is None:
            return
        if data.get('type') == 'table':
            table_name = data.get('name')
            self.loadTable(table_name)
    
    def loadTable(self, table_name):
        """
        Load data from the selected table into the table view.

        Args:
            table_name (str): Name of the table to load.

        Returns:
            bool: True if the table is loaded successfully, otherwise False.
        """
        try:
            self.current_table = table_name
            self.tableLabel.setText(f"Table: {table_name}")
            
            # Create and assign the table model.
            self.current_model = SqlTableModel(self.db_path, table_name, self)
            self.tableView.setModel(self.current_model)
            
            # Enable search and limit controls.
            self.searchBox.setEnabled(True)
            self.limitBox.setEnabled(True)
            
            # Adjust column widths.
            self.tableView.resizeColumnsToContents()
            return True
        except Exception as e:
            self.logger.error(f"Error loading table {table_name}: {e}")
            return False
    
    def onSearch(self):
        """
        Execute a search query on the current table.

        Uses a simple WHERE clause to search across all columns for the given text.
        """
        if not self.current_model or not self.current_table:
            return
            
        search_text = self.searchBox.text().strip()
        if not search_text:
            # If the search box is empty, refresh without filtering.
            self.current_model.load_data(limit=self.limitBox.value())
            return
            
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Retrieve column names for the current table.
            cursor.execute(f"PRAGMA table_info({self.current_table})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Build a WHERE clause that searches each column.
            where_clauses = []
            for col in columns:
                # Note: This approach may be slow for large tables.
                where_clauses.append(f"{col} LIKE '%{search_text}%'")
            where_clause = " OR ".join(where_clauses)
            
            # Load data using the built WHERE clause.
            self.current_model.load_data(limit=self.limitBox.value(), where_clause=where_clause)
            conn.close()
        except Exception as e:
            self.logger.error(f"Error performing search: {e}")
    
    def onLimitChanged(self, value):
        """
        Handle changes in the limit spinner and reload data accordingly.

        Args:
            value (int): The new limit value.
        """
        if self.current_model and self.current_table:
            # If there's an active search, reapply it.
            search_text = self.searchBox.text().strip()
            if search_text:
                self.onSearch()
            else:
                self.current_model.load_data(limit=value)

    def update_icon_colors(self, is_dark_mode):
        """
        Update the icon colors in the UI based on the current theme.

        Args:
            is_dark_mode (bool): True if dark mode is active, otherwise False.
        """
        icon_color = "white" if is_dark_mode else "black"
        
        # Update icons for the top-level tree item (e.g., "Tables").
        for i in range(self.tree.topLevelItemCount()):
            top_item = self.tree.topLevelItem(i)
            if top_item.text(0) == "Tables":
                top_item.setIcon(0, qta.icon('fa5s.table', color=icon_color))
                
            # Update icons for all table and column items.
            for table_idx in range(top_item.childCount()):
                table_item = top_item.child(table_idx)
                table_item.setIcon(0, qta.icon('fa5s.th-list', color=icon_color))
                for col_idx in range(table_item.childCount()):
                    col_item = table_item.child(col_idx)
                    col_item.setIcon(0, qta.icon('fa5s.list-alt', color=icon_color))
        
        self.tree.update()
