-- Create user_push_subscriptions table for browser push notifications
CREATE TABLE IF NOT EXISTS public.user_push_subscriptions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
    endpoint TEXT NOT NULL UNIQUE,
    p256dh TEXT NOT NULL,
    auth TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

-- Add index for performance
CREATE INDEX IF NOT EXISTS user_push_subscriptions_user_id_idx ON public.user_push_subscriptions(user_id);

-- Enable RLS
ALTER TABLE public.user_push_subscriptions ENABLE ROW LEVEL SECURITY;

-- RLS Policy: Users can only see/manage their own subscriptions
CREATE POLICY "Users can manage their own push subscriptions" 
ON public.user_push_subscriptions 
FOR ALL 
USING (auth.uid() = user_id);

-- Enable Realtime (optional, but good for debugging)
ALTER PUBLICATION supabase_realtime ADD TABLE user_push_subscriptions;
