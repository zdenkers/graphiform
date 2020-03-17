require_relative 'boot'

require 'rails/all'

Bundler.require(*Rails.groups)
require "graphiform"

module Dummy
  class Application < Rails::Application
    # Initialize configuration defaults for originally generated Rails version.
    # TODO: Rails 5 - Uncomment
    # config.load_defaults 5.2

    # Settings in config/environments/* take precedence over those specified here.
    # Application configuration can go into files in config/initializers
    # -- all .rb files in that directory are automatically loaded after loading
    # the framework and any gems in your application.
  end
end

