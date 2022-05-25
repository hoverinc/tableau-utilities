"""
    Permissions sets are the combination of capabilities
    that can be generally applied for a particular group/project
"""

deny_all = {
    # Project
    'Project_Read_View': 'Deny',
    'Project_Write': 'Deny',
    'Project_ProjectLeader': 'Deny',
    # Workbook VIew
    'Workbook_Read_View': 'Deny',
    'Workbook_ExportImage': 'Deny',
    'Workbook_ExportData': 'Deny',
    'Workbook_ViewComments': 'Deny',
    'Workbook_AddComment': 'Deny',
    # Workbook Interact
    'Workbook_Filter': 'Deny',
    'Workbook_ViewUnderlyingData': 'Deny',
    'Workbook_ShareView': 'Deny',
    'Workbook_WebAuthoring': 'Deny',
    # Workbook Edit
    'Workbook_Write': 'Deny',
    'Workbook_ExportXml': 'Deny',
    'Workbook_ChangeHierarchy': 'Deny',
    'Workbook_Delete': 'Deny',
    'Workbook_ChangePermissions': 'Deny',
    # Datasource Use
    'Datasource_Read_View': 'Deny',
    'Datasource_Connect': 'Deny',
    'Datasource_Write': 'Deny',
    'Datasource_ExportXml': 'Deny',
    # Datasource Edit
    'Datasource_Delete': 'Deny',
    'Datasource_ChangePermissions': 'Deny'
}

monitor = {
    # Project
    'Project_Read_View': 'Allow',
    'Project_Write': 'Deny',
    'Project_ProjectLeader': 'Deny',
    # Workbook VIew
    'Workbook_Read_View': 'Allow',
    'Workbook_ExportImage': 'Allow',
    'Workbook_ExportData': 'Deny',
    'Workbook_ViewComments': 'Deny',
    'Workbook_AddComment': 'Deny',
    # Workbook Interact
    'Workbook_Filter': 'Allow',
    'Workbook_ViewUnderlyingData': 'Deny',
    'Workbook_ShareView': 'Allow',
    'Workbook_WebAuthoring': 'Deny',
    # Workbook Edit
    'Workbook_Write': 'Deny',
    'Workbook_ExportXml': 'Deny',
    'Workbook_ChangeHierarchy': 'Deny',
    'Workbook_Delete': 'Deny',
    'Workbook_ChangePermissions': 'Deny',
    # Datasource Use
    'Datasource_Read_View': None,
    'Datasource_Connect': None,
    'Datasource_Write': None,
    'Datasource_ExportXml': None,
    # Datasource Edit
    'Datasource_Delete': None,
    'Datasource_ChangePermissions': None
}

internal_publisher = {
    # Project
    'Project_Read_View': 'Allow',
    'Project_Write': 'Allow',
    'Project_ProjectLeader': 'Deny',
    # Workbook VIew
    'Workbook_Read_View': 'Allow',
    'Workbook_ExportImage': 'Allow',
    'Workbook_ExportData': 'Allow',
    'Workbook_ViewComments': 'Allow',
    'Workbook_AddComment': 'Allow',
    # Workbook Interact
    'Workbook_Filter': 'Allow',
    'Workbook_ViewUnderlyingData': 'Allow',
    'Workbook_ShareView': 'Allow',
    'Workbook_WebAuthoring': 'Allow',
    # Workbook Edit
    'Workbook_Write': 'Allow',
    'Workbook_ExportXml': 'Allow',
    'Workbook_ChangeHierarchy': 'Allow',
    'Workbook_Delete': 'Allow',
    'Workbook_ChangePermissions': 'Deny',
    # Datasource Use
    'Datasource_Read_View': 'Allow',
    'Datasource_Connect': 'Allow',
    'Datasource_Write': None,
    'Datasource_ExportXml': 'Allow',
    # Datasource Edit
    'Datasource_Delete': 'Deny',
    'Datasource_ChangePermissions': 'Deny'
}

business_user = {
    # Project
    'Project_Read_View': 'Allow',
    'Project_Write': 'Allow',
    'Project_ProjectLeader': 'Deny',
    # Workbook VIew
    'Workbook_Read_View': 'Allow',
    'Workbook_ExportImage': 'Allow',
    'Workbook_ExportData': 'Allow',
    'Workbook_ViewComments': 'Allow',
    'Workbook_AddComment': 'Allow',
    # Workbook Interact
    'Workbook_Filter': 'Allow',
    'Workbook_ViewUnderlyingData': 'Allow',
    'Workbook_ShareView': 'Allow',
    'Workbook_WebAuthoring': 'Allow',
    # Workbook Edit
    'Workbook_Write': None,
    'Workbook_ExportXml': 'Allow',
    'Workbook_ChangeHierarchy': 'Allow',
    'Workbook_Delete': 'Deny',
    'Workbook_ChangePermissions': 'Deny',
    # Datasource Use
    'Datasource_Read_View': 'Allow',
    'Datasource_Connect': 'Allow',
    'Datasource_Write': 'Deny',
    'Datasource_ExportXml': 'Deny',
    # Datasource Edit
    'Datasource_Delete': 'Deny',
    'Datasource_ChangePermissions': 'Deny'
}

read_data_source_only = {
    # Project
    'Project_Read_View': 'Deny',
    'Project_Write': 'Deny',
    'Project_ProjectLeader': 'Deny',
    # Workbook VIew
    'Workbook_Read_View': 'Deny',
    'Workbook_ExportImage': 'Deny',
    'Workbook_ExportData': 'Deny',
    'Workbook_ViewComments': 'Deny',
    'Workbook_AddComment': 'Deny',
    # Workbook Interact
    'Workbook_Filter': 'Deny',
    'Workbook_ViewUnderlyingData': 'Deny',
    'Workbook_ShareView': 'Deny',
    'Workbook_WebAuthoring': 'Deny',
    # Workbook Edit
    'Workbook_Write': 'Deny',
    'Workbook_ExportXml': 'Deny',
    'Workbook_ChangeHierarchy': 'Deny',
    'Workbook_Delete': 'Deny',
    'Workbook_ChangePermissions': 'Deny',
    # Datasource Use
    'Datasource_Read_View': 'Allow',
    'Datasource_Connect': 'Allow',
    'Datasource_Write': 'Deny',
    'Datasource_ExportXml': 'Deny',
    # Datasource Edit
    'Datasource_Delete': 'Deny',
    'Datasource_ChangePermissions': 'Deny'
}
