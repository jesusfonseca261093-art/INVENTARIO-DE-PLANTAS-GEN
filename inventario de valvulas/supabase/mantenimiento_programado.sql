create table if not exists public.mantenimiento_programado (
  id text primary key,
  unidad_id text not null references public.at_units(id) on delete cascade,
  economico text not null,
  planta text,
  fecha_inicio date,
  fecha_fin date,
  prioridad numeric(10,2) not null default 0,
  estado text not null default 'DISPONIBLE',
  tecnico text,
  observaciones text,
  riesgo_operativo integer not null default 0,
  componentes_vencidos text,
  created_at timestamptz not null default now()
);

alter table public.mantenimiento_programado enable row level security;

drop policy if exists "mantenimiento_programado_all" on public.mantenimiento_programado;
create policy "mantenimiento_programado_all"
on public.mantenimiento_programado
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

create index if not exists idx_mantenimiento_programado_unidad_id
  on public.mantenimiento_programado (unidad_id);

create index if not exists idx_mantenimiento_programado_estado
  on public.mantenimiento_programado (estado);

create index if not exists idx_mantenimiento_programado_fecha_inicio
  on public.mantenimiento_programado (fecha_inicio);
