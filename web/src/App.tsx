import { Flex } from '@chakra-ui/react';
import { useForm } from "react-hook-form";
import { z } from "zod";
import { Loader2 } from "lucide-react";
import { toast } from "react-toastify";
import { zodResolver } from "@hookform/resolvers/zod";
import { Form, FormField, FormItem, FormMessage, FormControl } from "@/components/ui/form";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useEffect, useState } from 'react';
import { KeyValueResponse } from './types';
import MainApi from './api';

export enum MethodTypeString {
  PING = "Ping",
  GET = "Get",
  SET = "Set",
  DEL = "Del",
  STRLN = "Strln",
  APPEND = "Append",
  REQUESTLOG = "Request Log"
}

const createFormSchema = (disableKey: boolean, disableValue: boolean) => {
  return z.object({
    key: z.string().refine(val => disableKey || val.length > 0, {
      message: "Key is required and cannot be empty.",
    }),
    value: z.string().refine(val => disableValue || val.length > 0, {
      message: "Value is required and cannot be empty.",
    }),
  });
};

function App() {
  const [onUpdate, setOnUpdate] = useState<boolean>(false);
  const [disableKey, setDisableKey] = useState(true);
  const [disableValue, setDisableValue] = useState(true);
  const [commandType, setCommandType] = useState<string>(MethodTypeString.PING);
  const [commands, setCommands] = useState<{ type: MethodTypeString; key: string; value: string }[]>([]);

  const form = useForm({
    resolver: zodResolver(createFormSchema(disableKey, disableValue)),
    defaultValues: {
      key: "",
      value: ""
    },
  });

  useEffect(() => {
    form.reset(
      {},
      {
        keepErrors: true,
        keepDirty: true,
        keepValues: true,
      }
    );
  }, [disableKey, disableValue]);

  const handleMethodChange = (type: string) => {
    const value = type as MethodTypeString;
    setCommandType(value);
    setDisableKey(value === MethodTypeString.PING || value === MethodTypeString.REQUESTLOG);
    setDisableValue(
      value === MethodTypeString.PING ||
      value === MethodTypeString.REQUESTLOG ||
      value === MethodTypeString.GET ||
      value === MethodTypeString.STRLN ||
      value === MethodTypeString.DEL
    );
  };

  const handleAddCommand = () => {
    const { key, value } = form.getValues();
    setCommands([...commands, { type: commandType as MethodTypeString, key, value }]);
    form.reset({ key: "", value: "" }, { keepErrors: true });
  };

  const executeCommand = async (command: { type: MethodTypeString; key: string; value: string }) => {
    let commandValue = "";

    switch (command.type) {
      case MethodTypeString.PING:
        commandValue = "ping";
        break;
      case MethodTypeString.GET:
        commandValue = `get ${command.key}`;
        break;
      case MethodTypeString.SET:
        commandValue = `set ${command.key} ${command.value}`;
        break;
      case MethodTypeString.STRLN:
        commandValue = `strln ${command.key}`;
        break;
      case MethodTypeString.DEL:
        commandValue = `del ${command.key}`;
        break;
      case MethodTypeString.APPEND:
        commandValue = `append ${command.key} ${command.value}`;
        break;
      default:
        throw new Error("Unknown command type");
    }

    const address = {
      ip: "localhost",
      port: 8000
    };

    const payload = {
      address: address,
      command: commandValue,
    };

    const response: KeyValueResponse = await MainApi.request(payload);
    return response.data;
  };

  async function onSubmit() {
    try {
      setOnUpdate(true);
      const results = [];

      for (const command of commands) {
        const result = await executeCommand(command);
        results.push(result);
      }

      console.log(results);
      toast.success(results.join("\n"));
      setCommands([]); // Clear commands after submission
    } catch (error) {
      console.error("Login error:", error);
      toast.error("An error occurred while processing commands.");
    } finally {
      setOnUpdate(false);
    }
  }

  return (
    <Flex direction="column" h="100vh">
      <Flex as="header" backgroundColor="blue.500" backdropFilter="saturate(180%) blur(5px)" p={5} alignItems="center" w="100vw">
        <Button>Sistressss</Button>
      </Flex>
      <div className='flex flex-col justify-center items-center h-full space-y-6'>
        <div className='flex flex-col justify-center'>
          <div className='text-3xl font-bold text-center'>Key-value Store</div>
          <div className='text-xl mt-3'>Add new key-value pair to our system!</div>
        </div>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(handleAddCommand)} className="w-full space-y-4 justify-center">
            <div className='flex flex-row justify-center items-center px-32 w-full'>
              <div className='w-1/4'>
                <Select value={commandType} onValueChange={handleMethodChange}>
                  <SelectTrigger className="w-full h-10 bg-gray-800 border-none text-white">
                    <SelectValue placeholder="Choose Method" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value={MethodTypeString.PING}>Ping</SelectItem>
                    <SelectItem value={MethodTypeString.GET}>Get</SelectItem>
                    <SelectItem value={MethodTypeString.SET}>Set</SelectItem>
                    <SelectItem value={MethodTypeString.STRLN}>Strln</SelectItem>
                    <SelectItem value={MethodTypeString.DEL}>Del</SelectItem>
                    <SelectItem value={MethodTypeString.APPEND}>Append</SelectItem>
                    <SelectItem value={MethodTypeString.REQUESTLOG}>Request Log</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <FormField
                control={form.control}
                name="key"
                render={({ field }) => (
                  <FormItem className='w-1/4 rounded-2xl'>
                    <FormControl>
                      <Input placeholder="Key" {...field} disabled={disableKey} className="md:text-sm text-base border-black" />
                    </FormControl>
                    <FormMessage className="text-left" />
                  </FormItem>
                )}
              />
              <FormField
                control={form.control}
                name="value"
                render={({ field }) => (
                  <FormItem className='w-1/4 rounded-2xl'>
                    <FormControl>
                      <Input placeholder="Value" {...field} disabled={disableValue} className="md:text-sm text-base border-black" />
                    </FormControl>
                    <FormMessage className="text-left" />
                  </FormItem>
                )}
              />
              <Button type="submit" className="ml-4">Add Command</Button>
            </div>
          </form>
        </Form>
        <div className="w-full px-32">
          <h3 className="text-lg font-semibold mb-2">Commands:</h3>
          <ul className="list-disc pl-5 space-y-2">
            {commands.map((cmd, index) => (
              <li key={index} className="bg-gray-200 p-2 rounded-lg">
                {cmd.type} {cmd.key} {cmd.value}
              </li>
            ))}
          </ul>
          <Button
            onClick={onSubmit}
            className="justify-center w-full font-semibold h-fit rounded-3xl text-lg mt-8 mb-3 py-1.5 transition-transform duration-300 transform hover:scale-105"
            disabled={onUpdate}
          >
            {onUpdate ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Submitting
              </>
            ) : (
              'Submit All Commands'
            )}
          </Button>
        </div>
      </div>
    </Flex>
  )
}

export default App;