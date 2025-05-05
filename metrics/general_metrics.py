from openpyxl.utils import get_column_letter
from openpyxl import load_workbook

def expand_excel(excel_path: str):
    """
    Expands the columns visually in Excel such that users does not have to manually adjust the width.

    :param excel_path: The path to the Excel file.
    """
    # Load the workbook and get the active sheet
    wb = load_workbook(excel_path)
    ws = wb.active

    # Auto-adjust column width based on the max length in each column
    for col_idx, column_cells in enumerate(ws.columns, start=1):
        max_length = 0
        column = get_column_letter(col_idx)
        for cell in column_cells:
            try:
                cell_value = str(cell.value)
                if cell_value:
                    max_length = max(max_length, len(cell_value))
            except:
                pass
        adjusted_width = max_length + 2  # add a little padding
        ws.column_dimensions[column].width = adjusted_width

    # Save the updated workbook
    wb.save(excel_path)