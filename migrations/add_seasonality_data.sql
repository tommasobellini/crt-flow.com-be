-- Add seasonality_data column to crt_signals
ALTER TABLE public.crt_signals 
ADD COLUMN IF NOT EXISTS seasonality_data JSONB DEFAULT '{}'::jsonb;
