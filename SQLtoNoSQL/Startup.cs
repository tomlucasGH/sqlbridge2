using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(SQLtoNoSQL.Startup))]
namespace SQLtoNoSQL
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
