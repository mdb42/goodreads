from PyQt6.QtWidgets import (
    QWidget, QSplitter, QVBoxLayout, QTreeWidget, QTreeWidgetItem, 
    QTableView, QHeaderView, QLabel, QHBoxLayout, QLineEdit, QSpinBox
)
from PyQt6.QtCore import Qt, QAbstractTableModel, QVariant
from PyQt6.QtGui import QFont

import qtawesome as qta
import logging
import sqlite3

class SqlTableModel(QAbstractTableModel):
    """A model that displays data from an SQLite table."""
    
    def __init__(self, db_path, table_name, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger(__name__)
        self.conn = sqlite3.connect(db_path)
        self.table_name = table_name
        self.headers = []
        self.data_types = []
        self.data = []
        self.load_data()
    
    def load_data(self, limit=1000, where_clause=None):
        """Load data from the SQLite table with optional filtering."""
        try:
            # Get column names and types
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({self.table_name})")
            columns_info = cursor.fetchall()
            self.headers = [col[1] for col in columns_info]  # col[1] is the column name
            self.data_types = [col[2] for col in columns_info]  # col[2] is the data type
            
            # Print debug info
            print(f"Table: {self.table_name}, Columns: {self.headers}")
            
            # Build the query
            query = f"SELECT * FROM {self.table_name}"
            if where_clause:
                query += f" WHERE {where_clause}"
            query += f" LIMIT {limit}"
            
            print(f"Executing query: {query}")
            
            # Fetch the data
            cursor.execute(query)
            self.data = cursor.fetchall()
            
            print(f"Fetched {len(self.data)} rows")
            
            # Report column sample 
            if self.data and len(self.data) > 0:
                print(f"Sample row: {self.data[0]}")
                
            self.layoutChanged.emit()
            return True
        except Exception as e:
            self.logger.error(f"Error loading data from {self.table_name}: {e}")
            print(f"Error loading data: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def rowCount(self, parent=None):
        return len(self.data)
    
    def columnCount(self, parent=None):
        return len(self.headers)
    
    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self.data)):
            return QVariant()
        
        value = self.data[index.row()][index.column()]
        
        if role == Qt.ItemDataRole.DisplayRole:
            # Format based on data type
            if value is None:
                return "NULL"
            elif isinstance(value, (int, float)):
                return str(value)
            elif isinstance(value, str) and len(value) > 100:
                return value[:100] + "..."  # Truncate long text
            return str(value)
            
        elif role == Qt.ItemDataRole.TextAlignmentRole:
            # Align numbers right, text left
            if isinstance(value, (int, float)):
                return int(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            return int(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        
        return QVariant()
    
    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            if 0 <= section < len(self.headers):
                return self.headers[section]
        return QVariant()
    
    def refresh(self):
        """Reload data from the table."""
        return self.load_data()
    
    def getColumnDataType(self, column_index):
        """Get the SQLite data type for a column."""
        if 0 <= column_index < len(self.data_types):
            return self.data_types[column_index]
        return None

class DataBrowser(QWidget):
    """Main data browser widget with navigation tree and table view."""

    def __init__(self, db_path, parent=None):
        super().__init__(parent)
        self.logger = logging.getLogger(__name__)
        self.db_path = db_path
        self.current_table = None
        self.current_model = None
        
        # Ensure the database path exists
        self.logger.info(f"Initializing DataBrowser with database path: {db_path}")
        
        # Verify database connection
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
        """Initialize the UI components."""
        # Main layout
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Create a splitter for left navigation and right content
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.splitter.setChildrenCollapsible(False)
        
        # Left side - Navigation Tree
        self.navWidget = QWidget()
        navLayout = QVBoxLayout(self.navWidget)
        
        # Header for navigation
        navHeader = QHBoxLayout()
        navLabel = QLabel("Database Explorer")
        navLabel.setStyleSheet("font-weight: bold; font-size: 14px;")
        navHeader.addWidget(navLabel)
        navHeader.addStretch()
        navLayout.addLayout(navHeader)
        
        # Tree widget for navigation
        self.tree = QTreeWidget()
        self.tree.setHeaderHidden(True)
        self.tree.itemClicked.connect(self.onTreeItemClicked)
        navLayout.addWidget(self.tree)
        
        # Right side - Content area
        self.contentWidget = QWidget()
        contentLayout = QVBoxLayout(self.contentWidget)
        
        # Table header with controls
        self.tableHeader = QHBoxLayout()
        self.tableLabel = QLabel("Select a table to view data")
        self.tableLabel.setStyleSheet("font-weight: bold; font-size: 14px;")
        self.tableHeader.addWidget(self.tableLabel)
        self.tableHeader.addStretch()
        
        # Search control
        self.searchBox = QLineEdit()
        self.searchBox.setPlaceholderText("Search...")
        self.searchBox.setClearButtonEnabled(True)
        self.searchBox.setFixedWidth(200)
        self.searchBox.setEnabled(False)
        self.searchBox.returnPressed.connect(self.onSearch)
        self.tableHeader.addWidget(self.searchBox)
        
        # Limit selector
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
        
        # Table view
        self.tableView = QTableView()
        self.tableView.setSortingEnabled(False) # Disable sorting for now
        self.tableView.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.tableView.verticalHeader().setVisible(True)
        contentLayout.addWidget(self.tableView)
        
        # Add widgets to splitter
        self.splitter.addWidget(self.navWidget)
        self.splitter.addWidget(self.contentWidget)
        
        # Set splitter sizes to make the right panel larger
        self.splitter.setSizes([200, 600])
        
        layout.addWidget(self.splitter)

    def loadDatabaseSchema(self):
        """Load the database schema into the navigation tree."""
        self.tree.clear()
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Tables category
            tables_item = QTreeWidgetItem(self.tree, ["Tables"])
            tables_item.setIcon(0, qta.icon('fa5s.table'))
            tables_item.setFont(0, QFont(self.font().family(), self.font().pointSize(), QFont.Weight.Bold))
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = cursor.fetchall()
            
            self.logger.info(f"Found {len(tables)} tables in database")
            
            for table in tables:
                table_name = table[0]
                # Skip SQLite internal tables
                if table_name.startswith('sqlite_'):
                    continue
                    
                table_item = QTreeWidgetItem(tables_item, [table_name])
                table_item.setIcon(0, qta.icon('fa5s.th-list'))
                table_item.setData(0, Qt.ItemDataRole.UserRole, {'type': 'table', 'name': table_name})
                
                # Get column info for this table
                try:
                    cursor.execute(f"PRAGMA table_info({table_name})")
                    columns = cursor.fetchall()
                    
                    self.logger.info(f"Table {table_name} has {len(columns)} columns")
                    
                    for col in columns:
                        col_name = col[1]
                        col_type = col[2]
                        is_pk = col[5] == 1  # col[5] is 1 if column is primary key
                        
                        # Compose a display string for the column that includes its type
                        col_text = f"{col_name} ({col_type})"
                        if is_pk:
                            col_text += " 🔑"
                            
                        col_item = QTreeWidgetItem(table_item, [col_text])
                        col_item.setIcon(0, qta.icon('fa5s.columns'))
                        col_item.setData(0, Qt.ItemDataRole.UserRole, 
                                        {'type': 'column', 'name': col_name, 'table': table_name, 'data_type': col_type})
                except Exception as e:
                    self.logger.error(f"Error loading columns for table {table_name}: {e}")
            
            # Expand tables by default
            tables_item.setExpanded(True)
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error loading database schema: {e}")
            import traceback
            traceback.print_exc()

    def onTreeItemClicked(self, item, column):
        """Handle item click in the navigation tree."""
        data = item.data(0, Qt.ItemDataRole.UserRole)
        if data is None:
            return
            
        if data.get('type') == 'table':
            table_name = data.get('name')
            self.loadTable(table_name)
    
    def loadTable(self, table_name):
        """Load a table's data into the table view."""
        try:
            self.current_table = table_name
            self.tableLabel.setText(f"Table: {table_name}")
            
            # Create and set the table model
            self.current_model = SqlTableModel(self.db_path, table_name, self)
            self.tableView.setModel(self.current_model)
            
            # Enable controls
            self.searchBox.setEnabled(True)
            self.limitBox.setEnabled(True)
            
            # Make columns resize to content
            self.tableView.resizeColumnsToContents()
            
            # Success - return true
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading table {table_name}: {e}")
            return False
    
    def onSearch(self):
        """Handle search box queries."""
        if not self.current_model or not self.current_table:
            return
            
        search_text = self.searchBox.text().strip()
        if not search_text:
            # If search box is empty, just refresh with no filter
            self.current_model.load_data(limit=self.limitBox.value())
            return
            
        # Build a simple WHERE clause that searches all columns
        # This is a simplified approach and might be slow for large tables
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get column names for the current table
            cursor.execute(f"PRAGMA table_info({self.current_table})")
            columns = [col[1] for col in cursor.fetchall()]
            
            # Build WHERE clause to search in each column
            where_clauses = []
            for col in columns:
                where_clauses.append(f"{col} LIKE '%{search_text}%'")
            
            where_clause = " OR ".join(where_clauses)
            
            # Apply the search filter
            self.current_model.load_data(limit=self.limitBox.value(), where_clause=where_clause)
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error performing search: {e}")
    
    def onLimitChanged(self, value):
        """Handle changes to the result limit spinner."""
        if self.current_model and self.current_table:
            # Reload with new limit
            search_text = self.searchBox.text().strip()
            if search_text:
                self.onSearch()  # This will apply both search and limit
            else:
                self.current_model.load_data(limit=value)

    def update_icon_colors(self, is_dark_mode):
        """Update icon colors based on theme."""
        icon_color = "white" if is_dark_mode else "black"
        
        # Update tree widget icons
        for i in range(self.tree.topLevelItemCount()):
            top_item = self.tree.topLevelItem(i)
            
            # Update category icon (Tables)
            if top_item.text(0) == "Tables":
                top_item.setIcon(0, qta.icon('fa5s.table', color=icon_color))
                
            # Update table and column icons for all children
            for table_idx in range(top_item.childCount()):
                table_item = top_item.child(table_idx)
                table_item.setIcon(0, qta.icon('fa5s.th-list', color=icon_color))
                
                for col_idx in range(table_item.childCount()):
                    col_item = table_item.child(col_idx)
                    col_item.setIcon(0, qta.icon('fa5s.list-alt', color=icon_color))
        
        # Update any other icons in the UI
        self.tree.update()