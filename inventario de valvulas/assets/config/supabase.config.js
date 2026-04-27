window.SUPABASE_CONFIG = {
  // Pega aqui tu conexion de Supabase.
  // Ejemplo:
  // url: 'https://tu-proyecto.supabase.co',
  // anonKey: 'eyJhbGciOi...'
  url: 'https://xbnxplnuzdvmectvfbyk.supabase.co',
  anonKey: 'sb_publishable_O1fqCEeiovYXlRdRCX7AOA_ZG0XhwYE',

  // Opcional: cambiar nombres de tablas (si usas otras).
  tableUnits: 'at_units',
  tableRecords: 'at_records',
  tablePartImages: 'at_part_images',

  // Bucket de Supabase Storage para documentos del expediente.
  // Debe existir en Storage > Buckets (o crearse con el SQL de setup).
  bucketExpediente: 'at_expediente'
};
