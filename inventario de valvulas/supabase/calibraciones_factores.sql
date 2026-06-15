create table if not exists public.calibraciones_factores (
  id text primary key,
  tipo text not null check (tipo in ('estaciones', 'autotanques')),
  planta text not null,
  equipo_id text not null,
  nombre text,
  fecha date not null,
  anio integer not null check (anio between 2000 and 2100),
  mes integer not null check (mes between 1 and 12),
  factor numeric(12,2) not null check (factor >= 0),
  observaciones text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint calibraciones_factores_fecha_mes_check
    check (
      anio = extract(year from fecha)::integer
      and mes = extract(month from fecha)::integer
    ),
  constraint calibraciones_factores_equipo_mes_unique
    unique (tipo, planta, equipo_id, anio, mes)
);

create or replace function public.set_calibraciones_factores_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists calibraciones_factores_set_updated_at on public.calibraciones_factores;
create trigger calibraciones_factores_set_updated_at
before update on public.calibraciones_factores
for each row
execute function public.set_calibraciones_factores_updated_at();

alter table public.calibraciones_factores enable row level security;

drop policy if exists "calibraciones_factores_all" on public.calibraciones_factores;
create policy "calibraciones_factores_all"
on public.calibraciones_factores
for all
using (auth.role() = 'authenticated')
with check (auth.role() = 'authenticated');

create index if not exists idx_calibraciones_factores_equipo
  on public.calibraciones_factores (tipo, planta, equipo_id);

create index if not exists idx_calibraciones_factores_anio_mes
  on public.calibraciones_factores (anio, mes);

create index if not exists idx_calibraciones_factores_fecha
  on public.calibraciones_factores (fecha desc);
