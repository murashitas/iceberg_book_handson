c = get_config()

c.SqlMagic.autopandas = True
c.SqlMagic.displaylimit = 50

c.InteractiveShellApp.extensions = [
    'sql'
]
