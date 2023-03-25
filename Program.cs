using Oakton;
using System.Reflection;


class Program
{
    static int Main(string[] args)
    {
        var executor = CommandExecutor.For(_ =>
        {
            _.RegisterCommands(typeof(Program).GetTypeInfo().Assembly);
        });

        return executor.Execute(args);
    }
}