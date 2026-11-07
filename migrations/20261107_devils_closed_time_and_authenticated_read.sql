-- Apply in Supabase SQL Editor. This migration does not recalculate historical trades.
BEGIN;

ALTER TABLE public.devils_signals DROP CONSTRAINT IF EXISTS devils_signals_status_check;
ALTER TABLE public.devils_signals ADD CONSTRAINT devils_signals_status_check CHECK (
  status IN ('ACTIVE', 'MARKER_PENDING', 'CONTINUATION_ACTIVE', 'INVALIDATED',
             'CLOSED_WIN', 'CLOSED_LOSS', 'CLOSED_TIME')
);

DROP POLICY IF EXISTS "devils_signals_read_all" ON public.devils_signals;
CREATE POLICY "devils_signals_read_all" ON public.devils_signals
  FOR SELECT TO authenticated USING (auth.uid() IS NOT NULL);

COMMIT;
