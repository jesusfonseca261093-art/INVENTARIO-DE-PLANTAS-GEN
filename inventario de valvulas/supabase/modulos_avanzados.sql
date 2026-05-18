create table if not exists public.inventario_refacciones (
  id text primary key,
  nombre text not null,
  numero_parte text not null,
  marca text,
  stock_actual integer not null default 0,
  stock_minimo integer not null default 0,
  ubicacion text,
  proveedor text,
  costo numeric(12,2) not null default 0,
  categoria text,
  created_at timestamptz not null default now()
);

create table if not exists public.movimientos_inventario (
  id text primary key,
  refaccion_id text not null references public.inventario_refacciones(id) on delete cascade,
  tipo_movimiento text not null,
  cantidad integer not null default 1,
  unidad text,
  tecnico text,
  fecha timestamptz not null default now(),
  observaciones text
);

create table if not exists public.auditoria_sistema (
  id text primary key,
  usuario text,
  accion text not null,
  modulo text not null,
  descripcion text not null,
  fecha timestamptz not null default now(),
  unidad text,
  evidencia text
);

alter table public.inventario_refacciones enable row level security;
alter table public.movimientos_inventario enable row level security;
alter table public.auditoria_sistema enable row level security;

drop policy if exists "inventario_refacciones_all" on public.inventario_refacciones;
create policy "inventario_refacciones_all"
on public.inventario_refacciones
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists "movimientos_inventario_all" on public.movimientos_inventario;
create policy "movimientos_inventario_all"
on public.movimientos_inventario
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

drop policy if exists "auditoria_sistema_all" on public.auditoria_sistema;
create policy "auditoria_sistema_all"
on public.auditoria_sistema
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

create index if not exists idx_refacciones_numero_parte on public.inventario_refacciones (numero_parte);
create index if not exists idx_movimientos_refaccion_fecha on public.movimientos_inventario (refaccion_id, fecha desc);
create index if not exists idx_auditoria_modulo_fecha on public.auditoria_sistema (modulo, fecha desc);
